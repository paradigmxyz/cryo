from __future__ import annotations

import argparse
import typing

import rich_argparse

if typing.TYPE_CHECKING:
    import typing_extensions


usage_template = """%(prog)s [{prog}]QUERY [OPTIONS][/{prog}]\n\n

[{help} not bold]This will 1. setup comparison dir, 2. collect data, and 3. compare outputs[/{help} not bold]

[{groups}]Examples:[/{groups}]
  [{prog}]cryo_test --rpc source1=https://h1.com source2=https://h2.com[/{prog}] (compare rpc's)
  [{prog}]cryo_test --executable old=/path/to/cryo1 new=/path/to/cryo2[/{prog}] (compare executables)
  [{prog}]cryo_test --python ...[/{prog}] (use python)
  [{prog}]cryo_test --cli-vs-python ...[/{prog}] (compare cli vs python)"""


class CryoTestHelpFormatter(rich_argparse.RichHelpFormatter):
    usage_markup = True

    styles = {
        'argparse.prog': 'bold white',
        'argparse.groups': 'bold rgb(0,255,0)',
        'argparse.args': 'bold white',
        'argparse.metavar': 'grey62',
        'argparse.help': 'grey62',
        'argparse.text': 'blue',
        'argparse.syntax': 'blue',
        'argparse.default': 'blue',
    }

    def __init__(self, prog: str) -> None:
        super().__init__('cryo_test', max_help_position=45)

    def _format_args(self, action, default_metavar):  # type: ignore
        get_metavar = self._metavar_formatter(action, default_metavar)
        if action.nargs == argparse.ZERO_OR_MORE:
            return '[%s [%s ...]]' % get_metavar(2)
        elif action.nargs == argparse.ONE_OR_MORE:
            return '%s [...]' % get_metavar(1)
        return super()._format_args(action, default_metavar)

    def format_help(self) -> str:
        import rich

        line = '[{prog}]cryo_test[/{prog}] [{help}]is a cli tool for comparing [{prog}]cryo[/{prog}] outputs across different conditions\n'
        rich.print(self.format_styles(line))

        # indent certain arguments for full alignment
        raw = super().format_help()
        lines = raw.split('\n')
        for i, line in enumerate(lines):
            if line.startswith('  \x1b[38;2;197;149;242m--'):
                lines[i] = '    ' + lines[i].replace('    ', '', 1)

        # indices = [i for i, line in enumerate(lines) if line.startswith('  \x1b[1;37m--')]
        # if lines[indices[0]] == '':
        #     lines = lines[:indices[0]] + ['FUCK'] + lines[indices[0]:]

        # lines = [
        #     line
        #     for line in lines
        #     if 'Options:' not in line and '--help' not in line
        # ]
        return '\n'.join(lines).replace('\n\n\n', '\n\n')

    @classmethod
    def format_styles(cls, s: str) -> str:
        styles = {k.split('.')[1]: v for k, v in cls.styles.items()}
        return s.format(**styles)


class CryoTestArgParser(argparse.ArgumentParser):
    def __init__(self, *args: typing.Any, **kwargs: typing.Any) -> None:
        return super().__init__(
            *args,
            formatter_class=CryoTestHelpFormatter,  # type: ignore
            usage=CryoTestHelpFormatter.format_styles(usage_template),
            **kwargs,
        )

    def error(self, message: str) -> typing_extensions.NoReturn:
        import sys
        import rich

        sys.stderr.write(f'Error: {message}\n')
        print()
        self.print_usage()
        print()
        line = '[{help}]show all options with[/{help}] [bold white]cryo_test -h[/bold white]'
        rich.print(CryoTestHelpFormatter.format_styles(line))
        sys.exit(0)
