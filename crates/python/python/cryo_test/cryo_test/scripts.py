from __future__ import annotations

from . import commands as commands_module


def generate_collect_script(
    commands: dict[str, str], script_path: str | None = None
) -> str:
    interface = commands_module.get_commands_interface(commands)
    if interface == 'cli':
        script = '#!/usr/bin/env bash\n\n'
        for name, command in commands.items():
            script += '\n\n# ' + name + '\n'
            script += 'echo "collecting ' + name + '"\n'
            script += 'echo ""\n'
            script += command.replace(' -', ' \\\n    -') + '\n'
            script += 'echo ""\n'
            script += 'echo ""\n'
    elif interface == 'python':
        script = '#!/usr/bin/env python3\n'
        for name, command in commands.items():
            script += '\n\n# ' + name + '\n'
            script += "print('collecting ' + name)"
            script += command
    else:
        raise Exception('invalid interface')

    # write to file
    if script_path is not None:
        _write_script(script, script_path)

    return script


def _write_script(script: str, path: str) -> None:
    import os
    import stat

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as f:
        f.write(script)
    st = os.stat(path)
    os.chmod(path, st.st_mode | stat.S_IXUSR)
