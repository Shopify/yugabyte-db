"""The developer interaction the `upstream.py` commands share: questions, stops and scratch files.

A command that needs the developer's input stops with exit 4 and says how to continue; on a
terminal it asks instead. Requires Python 3.10+; importing has no side effects.
"""

from __future__ import annotations

import os
from pathlib import Path
import shlex
import sys
import tempfile

from scm import Tool


UPSTREAM_SCRIPT = Path(__file__).resolve().with_name("upstream.py")


class Stop(Exception):
    """Stops for developer input (exit 4); `lines` say how to continue."""

    def __init__(self, *lines):
        super().__init__("\n".join(lines))
        self.lines = lines


class Interaction:
    """One run of a command that may ask the developer questions. Subclasses implement `main`.

    `tool`, `stdin`, `interactive`, `cwd`, `program` and `workdir` replace what the run would
    take from the process; tests pass them. `--yes` turns the questions off.
    """

    workdir_prefix = "upstream-"

    def __init__(self, args, out, err, say, tool=None, stdin=None, interactive=None, cwd=None,
                 program=None, workdir=None):
        self.args = args
        self.out = out
        self.err = err
        self.say = say
        self.tool = Tool() if tool is None else tool
        self.stdin = sys.stdin if stdin is None else stdin
        if interactive is None:
            interactive = sys.stdin.isatty() and sys.stdout.isatty()
        self.interactive = interactive and not getattr(args, "yes", False)
        self.cwd = os.getcwd() if cwd is None else cwd
        self.program = self.default_program() if program is None else program
        self.workdir_path = workdir

    def default_program(self):
        """`python3 <upstream.py>`, relative to the current directory when inside it."""
        try:
            shown = str(UPSTREAM_SCRIPT.relative_to(Path(self.cwd).resolve()))
        except ValueError:
            shown = str(UPSTREAM_SCRIPT)
        return f"python3 {shlex.quote(shown)}"

    def run(self):
        try:
            return self.main()
        except Stop as stop:
            self.say(*stop.lines)
            return 4

    def main(self):
        raise NotImplementedError

    def ask(self, prompt):
        """The developer's answer, stripped, or None at the end of input."""
        self.out.write(prompt)
        self.out.flush()
        line = self.stdin.readline()
        return line.strip() if line else None

    def agree(self, prompt):
        """Whether the developer answers y or yes; anything else, end of input included, is no."""
        return (self.ask(prompt) or "").lower() in ("y", "yes")

    def warn(self, line):
        self.say(line, stream=self.err)

    def workdir(self):
        """The directory generated files go to: a fresh temporary one unless given, kept."""
        if self.workdir_path is None:
            self.workdir_path = tempfile.mkdtemp(prefix=self.workdir_prefix)
        os.makedirs(self.workdir_path, exist_ok=True)
        return self.workdir_path

    def write(self, name, text):
        path = os.path.join(self.workdir(), name)
        Path(path).write_text(text, encoding="utf-8")
        return path
