from __future__ import annotations

import typing

from .. import commands
from .. import comparison
from .. import files
from . import cli_classes
from . import cli_summary
from . import cli_utils

if typing.TYPE_CHECKING:
    import argparse


def run_cli() -> None:
    try:
        # parse inputs
        args = parse_args()

        if set(args.steps) in [
            {'compare'},
            {'collect'},
            {'collect', 'compare'},
        ]:
            rerun = True
        else:
            rerun = args.rerun

        # load commands
        if rerun:
            if args.dir is not None:
                comparison_dir = args.dir
            else:
                comparison_dir = files.get_most_recent_comparison_dir()
            batches = files.load_commands(comparison_dir)
        else:
            comparison_dir, batches = create_command_batches(args)
            files.save_commands(comparison_dir=comparison_dir, batches=batches)

        # summarize
        cli_summary.summarize_args(args, comparison_dir, batches)

        # perform comparison
        comparison.perform_comparison(
            batches=batches,
            comparison_dir=comparison_dir,
            steps=args.steps,
        )

        # enter interactive session
        if args.interactive or args.scan_interactive:
            print('entering interactive session')
            if args.scan_interactive:
                all_data: typing.Any = files.scan_all_data(comparison_dir)
            else:
                all_data = files.load_all_data(comparison_dir)
            cli_utils.open_interactive_session({'data': all_data})
    except Exception as e:
        if args.debug:
            cli_utils._enter_debugger()
        else:
            raise e


def parse_args() -> argparse.Namespace:
    parser = cli_classes.CryoTestArgParser()
    #
    # specifying batch args
    parser.add_argument(
        '-e',
        '--executable',
        '--executables',
        nargs='+',
        help='executable(s) to use',
    )
    parser.add_argument('--rpc', nargs='+', help='rpc endpoint(s) to use')
    parser.add_argument(
        '-d',
        '--datatype',
        '--datatypes',
        nargs='+',
        help='datatype(s) to collect',
    )
    parser.add_argument(
        '-p', '--python', action='store_true', help='use python for all batches'
    )
    parser.add_argument(
        '--cli-vs-python', action='store_true', help='compare cli to python'
    )
    #
    # specifying how to run
    parser.add_argument(
        '-r', '--rerun', action='store_true', help='rerun previous comparison'
    )
    parser.add_argument('--label', help='name of comparison')
    parser.add_argument(
        '-s',
        '--steps',
        help='steps to perform {setup, collect, compare}',
        nargs='+',
        choices=['setup', 'collect', 'compare'],
        default=['setup', 'collect', 'compare'],
    )
    parser.add_argument('--dir', help='directory for storing comparison data')
    parser.add_argument(
        '-i',
        '--interactive',
        action='store_true',
        help='load data in interactive python session',
    )
    parser.add_argument(
        '--scan-interactive',
        action='store_true',
        help='scan data in interactive python session',
    )
    parser.add_argument(
        '--debug',
        '--pdb',
        action='store_true',
        help='enter debug mode upon error',
    )
    return parser.parse_args()


def create_command_batches(
    args: argparse.Namespace,
) -> tuple[str, dict[str, dict[str, str]]]:
    command_arg_combos: dict[str, list[str]] = {}
    common_command_args: commands.PartialCommandArgs = {}
    batch_specific_args: dict[str, commands.PartialCommandArgs] = {}

    # determine batch mode (the variables that change across batches)
    if args.executable is not None and len(args.executable) > 1:
        batch_mode = 'executable'
    elif args.rpc is not None and len(args.rpc) > 1:
        batch_mode = 'rpc'
    else:
        batch_mode = 'default'

    # determine comparison dir
    comparison_dir = files.create_comparison_dir(args.dir, args.label)

    # determine interface
    if args.python:
        common_command_args['interface'] = 'python'

    #
    if batch_mode == 'default':
        batch_specific_args = {'default': {}}

    # arg: executable
    if batch_mode == 'executable':
        for raw_executable in args.executable:
            batch_name, executable = raw_executable.split('=', maxsplit=1)
            batch_specific_args[batch_name] = {'executable': executable}
    else:
        if args.executable is None:
            common_command_args['executable'] = 'cryo'
        elif len(args.executable) == 1:
            common_command_args['executable'] = args.executable[0]
        else:
            raise Exception()

    # arg: rpc
    if batch_mode == 'rpc':
        for raw_rpc in args.rpcs:
            batch_name, rpc = raw_rpc.split('=', maxsplits=1)
            batch_specific_args[batch_name] = {'rpc': rpc}
    else:
        if args.rpc is None:
            pass
        elif len(args.rpc) == 1:
            common_command_args['rpc'] = args.rpc[0]
        else:
            raise Exception()

    # arg: datatypes
    if args.datatype is not None:
        command_arg_combos['datatype'] = args.datatype

    # if cli-vs-python, create cli and python version of each batch
    if args.cli_vs_python:
        new_batch_specific_args = {}
        for batch_name, batch_kwargs in batch_specific_args.items():
            new_batch_specific_args[batch_name + '_cli'] = dict(
                batch_kwargs, interface='cli'
            )
            new_batch_specific_args[batch_name + '_python'] = dict(
                batch_kwargs, interface='python'
            )

    # arg: output_dir
    for batch_name in batch_specific_args.keys():
        batch_specific_args[batch_name]['data_root'] = files.get_batch_data_dir(
            comparison_dir=comparison_dir,
            batch_name=batch_name,
        )

    # generate commands
    batches = {}
    for batch_name in batch_specific_args.keys():
        kwargs: commands.PartialCommandArgs = dict(
            **common_command_args, **batch_specific_args[batch_name]
        )
        batches[batch_name] = commands.generate_commands(
            common_command_args=kwargs,
            command_arg_combos=command_arg_combos,
        )

    return comparison_dir, batches

