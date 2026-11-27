"""Record a command's output as an asciicast v2 file, and render such a file to an MP4 video.

    python scripts/demo/cast.py record OUT.cast [--title T] -- COMMAND...
    python scripts/demo/cast.py render IN.cast OUT.mp4 [--max-idle S] [--ffmpeg PATH]

`record` runs COMMAND with its standard output and error on one pipe, echoes what it prints and
writes each chunk with its time since the start: what COMMAND prints, nothing else.

`render` replays the cast on a line-based screen (ANSI codes dropped, carriage returns
honoured), shortens to --max-idle every wait that exceeds it by a second or more and says so on
screen, writes the screens as ASS subtitles over a dark background, with the real and the video
clocks at the bottom, and encodes them with ffmpeg.
Standard library only.
"""

from __future__ import annotations

import argparse
import codecs
import json
import math
import os
import re
import subprocess
import sys
import tempfile
import textwrap
import time
from dataclasses import dataclass
from pathlib import Path

WIDTH, HEIGHT = 140, 30
FPS = 10
ANSI = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]|\x1b\][^\x07]*\x07")
VIDEO_W, VIDEO_H = 1280, 720
FONT, FONT_SIZE, LINE_H, LEFT, TOP = "Consolas", 16, 20, 18, 52


def record(out: Path, title: str, command: list[str]) -> int:
    start = time.monotonic()
    decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
    header = {
        "version": 2,
        "width": WIDTH,
        "height": HEIGHT,
        "timestamp": int(time.time()),
        "title": title,
        "env": {"SHELL": "/bin/bash", "TERM": "xterm-256color"},
    }
    with out.open("w", encoding="utf-8", newline="\n") as cast:
        cast.write(json.dumps(header, ensure_ascii=False) + "\n")
        child = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        fd = child.stdout.fileno()
        while chunk := os.read(fd, 65536):
            text = decoder.decode(chunk)
            if text:
                event = [round(time.monotonic() - start, 6), "o", text]
                cast.write(json.dumps(event, ensure_ascii=False) + "\n")
            sys.stdout.buffer.write(chunk)
            sys.stdout.flush()
        return child.wait()


@dataclass
class Frame:
    start: float
    end: float
    lines: list[str]
    real: float
    skipped: float


class Screen:
    """Lines printed so far; a carriage return rewrites the current line."""

    def __init__(self) -> None:
        self.done: list[str] = []
        self.current = ""
        self.pending_cr = False

    def feed(self, text: str) -> None:
        for char in ANSI.sub("", text).replace("\t", "    "):
            if char == "\n":
                self.done.append(self.current)
                self.current, self.pending_cr = "", False
            elif char == "\r":
                self.pending_cr = True
            else:
                if self.pending_cr:
                    self.current, self.pending_cr = "", False
                self.current += char

    def rows(self) -> list[str]:
        wrapped: list[str] = []
        for line in self.done[-HEIGHT:] + [self.current]:
            wrapped += textwrap.wrap(line, WIDTH, drop_whitespace=False) or [""]
        return wrapped[-HEIGHT:]


def frames(events: list[tuple[float, str]], max_idle: float) -> list[Frame]:
    """The screens to show. A wait is shortened only when that saves at least one second, so the
    notice never reads "20 s shortened to 20 s". The first frame is the empty screen, so a wait
    before the first output is shortened with its notice too."""
    screen, video, last = Screen(), 0.0, 0.0
    result = [Frame(0.0, 0.0, screen.rows(), 0.0, 0.0)]
    for t, text in events:
        gap = t - last
        skipped = gap - max_idle if gap - max_idle >= 1.0 else 0.0
        video += gap - skipped
        result[-1].end = video
        result[-1].skipped = skipped
        screen.feed(text)
        if round(result[-1].start * FPS) == round(video * FPS):
            result[-1].lines = screen.rows()
        else:
            result.append(Frame(video, video, screen.rows(), t, 0.0))
        last = t
    result[-1].end = video + 3.0
    return result


def ass_time(seconds: float) -> str:
    cs = round(seconds * 100)
    return f"{cs // 360000}:{cs // 6000 % 60:02d}:{cs // 100 % 60:02d}.{cs % 100:02d}"


def ass_text(line: str) -> str:
    """Escape the ASS override characters. Leading spaces become hard spaces: libass drops them."""
    text = line.replace("\\", "\\⁠").replace("{", "\\{").replace("}", "\\}")
    body = text.lstrip(" ")
    return "\\h" * (len(text) - len(body)) + body


def clock(seconds: float) -> str:
    return f"{int(seconds) // 60:02d}:{int(seconds) % 60:02d}"


def ticks(start: float, end: float) -> list[tuple[float, float]]:
    """Split [start, end) at each whole second of video."""
    cuts = [start, *range(int(start) + 1, math.ceil(end)), end]
    return [(a, b) for a, b in zip(cuts, cuts[1:]) if b > a]


def write_ass(path: Path, shots: list[Frame], title: str, max_idle: float) -> None:
    head = (
        f"{title}. Les attentes d'au moins {max_idle + 1:g} s sont raccourcies à {max_idle:g} s, "
        "et la durée réelle reste affichée en bas."
    )
    lines = [
        "[Script Info]",
        "ScriptType: v4.00+",
        f"PlayResX: {VIDEO_W}",
        f"PlayResY: {VIDEO_H}",
        "WrapStyle: 2",
        "",
        "[V4+ Styles]",
        "Format: Name, Fontname, Fontsize, PrimaryColour, OutlineColour, BackColour, Bold, "
        "BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV",
        f"Style: Term,{FONT},{FONT_SIZE},&H00E5E7EB,&H00000000,&H00000000,0,1,0,0,7,0,0,0",
        f"Style: Step,{FONT},{FONT_SIZE},&H0024BFFB,&H00000000,&H00000000,1,1,0,0,7,0,0,0",
        "Style: Head,Segoe UI,17,&H00F8FAFC,&H00000000,&H00000000,0,1,0,0,7,0,0,0",
        "Style: Wait,Segoe UI,22,&H00111827,&H0000D7FF,&H0000D7FF,1,3,8,0,2,0,0,0",
        "",
        "[Events]",
        "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text",
    ]
    end = shots[-1].end
    lines.append(f"Dialogue: 0,{ass_time(0)},{ass_time(end)},Head,,0,0,0,,{{\\pos({LEFT},12)}}{ass_text(head)}")
    for i, shot in enumerate(shots):
        if shot.end <= shot.start:
            continue
        span = f"{ass_time(shot.start)},{ass_time(shot.end)}"
        for row, text in enumerate(shot.lines):
            style = "Step" if text.startswith("== ") else "Term"
            pos = f"{{\\pos({LEFT},{TOP + row * LINE_H})}}"
            lines.append(f"Dialogue: 0,{span},{style},,0,0,0,,{pos}{ass_text(text)}")
        # The clocks tick every second. Over a shortened wait the real clock runs faster; after the
        # last output it stops.
        next_real = shots[i + 1].real if i + 1 < len(shots) else shot.real
        for a, b in ticks(shot.start, shot.end):
            real = shot.real + (next_real - shot.real) * (a - shot.start) / (shot.end - shot.start)
            footer = f"durée réelle {clock(real)}   vidéo {clock(a)}"
            lines.append(
                f"Dialogue: 0,{ass_time(a)},{ass_time(b)},Head,,0,0,0,,"
                f"{{\\an9\\pos({VIDEO_W - LEFT},{VIDEO_H - 30})}}{footer}"
            )
        if shot.skipped:
            notice = f"attente de {shot.skipped + max_idle:.0f} s raccourcie à {max_idle:g} s"
            wait = f"{ass_time(shot.end - max_idle)},{ass_time(shot.end)}"
            lines.append(f"Dialogue: 1,{wait},Wait,,0,0,0,,{{\\pos({VIDEO_W // 2},{VIDEO_H - 60})}}{notice}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def render(cast: Path, out: Path, max_idle: float, ffmpeg: str) -> int:
    with cast.open(encoding="utf-8") as handle:
        header = json.loads(handle.readline())
        events = [(e[0], e[2]) for e in map(json.loads, handle) if e[1] == "o"]
    shots = frames(events, max_idle)
    with tempfile.TemporaryDirectory() as tmp:
        ass = Path(tmp) / "screen.ass"
        write_ass(ass, shots, header.get("title", cast.stem), max_idle)
        command = [
            ffmpeg, "-y", "-loglevel", "error",
            "-f", "lavfi", "-i", f"color=c=0x0f172a:s={VIDEO_W}x{VIDEO_H}:r={FPS}:d={shots[-1].end:.2f}",
            "-vf", "ass=screen.ass",
            "-c:v", "libx264", "-preset", "slow", "-crf", "28", "-tune", "stillimage",
            "-pix_fmt", "yuv420p", "-movflags", "+faststart", str(out.resolve()),
        ]
        code = subprocess.run(command, cwd=tmp).returncode
    real = events[-1][0]
    print(f"{out}: {shots[-1].end:.1f} s of video for {real:.1f} s recorded")
    return code


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    split = argv.index("--") if "--" in argv else len(argv)
    argv, cmd = argv[:split], argv[split + 1 :]
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    rec = commands.add_parser("record")
    rec.add_argument("out", type=Path)
    rec.add_argument("--title", default="")
    ren = commands.add_parser("render")
    ren.add_argument("cast", type=Path)
    ren.add_argument("out", type=Path)
    ren.add_argument("--max-idle", type=float, default=2.0)
    ren.add_argument("--ffmpeg", default="ffmpeg")
    args = parser.parse_args(argv)
    if args.command == "record":
        if not cmd:
            parser.error("record needs a command after --")
        return record(args.out, args.title or " ".join(cmd), cmd)
    return render(args.cast, args.out, args.max_idle, args.ffmpeg)


if __name__ == "__main__":
    sys.exit(main())
