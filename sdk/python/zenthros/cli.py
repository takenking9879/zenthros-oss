# SPDX-License-Identifier: Apache-2.0

"""zenthros: the user's command line (usability R2).

    zenthros login                 log in (zenthros.login)
    zenthros attach my-vm          connect a machine (zenthros.attach)
    zenthros targets               where you can run (zenthros.targets)
    zenthros install my-vm -r requirements.txt
                                   packages for every function there (zenthros.install)
    zenthros store s3://my-bucket/zenthros
                                   your bucket for calls and results (zenthros.set_store)
    zenthros detach my-vm          disconnect it (zenthros.delete)
    zenthros run --on my-vm -- make test
                                   run a command, here if no --on (zenthros.run)
    zenthros jobs | status | logs | cancel
                                   what ran and what runs (--json: for a script)
    zenthros mcp                   the same for an agent, over MCP
    zenthros agent allow gcp-lab   let your agents use what costs money
    zenthros agent skill           tell your agents how, in this project
    zenthros ui                    the same on a page, for this computer

Each command is its Python twin, called the same way, so a notebook does
the same with zenthros.<twin>(...). What goes wrong is one line, "✗ ...",
that ends with what to do; the exit code is 1.
"""

import argparse
import json
import os
import sys

from zenthros import errors

_TTY = sys.stdout.isatty() and not os.environ.get("NO_COLOR")


def _bold(text):
    return f"\033[1m{text}\033[0m" if _TTY else text


def select(label, options, default=0, stdin=None, stdout=None):
    """The index of one of options. Arrows on a terminal; a number
    anywhere else (a notebook, a pipe). No question mark: a label."""
    stdin, stdout = stdin or sys.stdin, stdout or sys.stdout
    label = label.ljust(15)
    if stdin.isatty() and stdout.isatty():
        return _select_arrows(label, options, default)
    numbered = "  ".join(f"{i + 1}) {o}" for i, o in enumerate(options))
    while True:
        stdout.write(f"{label}› {numbered}  [{default + 1}]: ")
        stdout.flush()
        answer = stdin.readline()
        if not answer:
            raise errors.ValidationError(f"no answer for {label.strip()}: pass it as a flag (zenthros attach --help)")
        answer = answer.strip()
        if not answer:
            choice = default
        elif answer.isdigit() and 1 <= int(answer) <= len(options):
            choice = int(answer) - 1
        else:
            stdout.write(f"Type a number from 1 to {len(options)}.\n")
            continue
        stdout.write(f"{label}› {options[choice]}\n")
        return choice


def _select_arrows(label, options, index):
    import termios
    import tty

    def draw(final=False):
        if final:
            line = f"{label}› {_bold(options[index])}"
        else:
            parts = [(f"\033[7m {o} \033[0m" if i == index else f" {o} ") for i, o in enumerate(options)]
            line = f"{label}›" + " ".join(parts) + "   ←/→, Enter"
        sys.stdout.write("\r\033[2K" + line)
        sys.stdout.flush()

    fd = sys.stdin.fileno()
    saved = termios.tcgetattr(fd)
    try:
        tty.setcbreak(fd)
        draw()
        while True:
            ch = sys.stdin.read(1)
            if ch in ("\r", "\n"):
                break
            if ch == "\x03":
                raise KeyboardInterrupt
            if ch == "\x1b":
                seq = sys.stdin.read(2)
                if seq == "[C":
                    index = (index + 1) % len(options)
                elif seq == "[D":
                    index = (index - 1) % len(options)
            elif ch.isdigit() and 1 <= int(ch) <= len(options):
                index = int(ch) - 1
            draw()
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, saved)
    draw(final=True)
    sys.stdout.write("\n")
    return index


def parser():
    p = argparse.ArgumentParser(prog="zenthros", description="Run your code on your machines.")
    sub = p.add_subparsers(dest="cmd", metavar="<command>")

    login = sub.add_parser("login", help="log in")
    login.add_argument("--key", help="an API key the hub's administrator gave you (- reads it from stdin, "
                       "so it stays out of the shell's history)")
    login.add_argument("--server", metavar="URL", help="no key: sign in with Google on this hub, for a Workspace of your own")
    login.add_argument("--ca-file", metavar="FILE", help="the hub's CA, when its certificate is not a public one")
    login.add_argument("--google", action="store_true", help="sign in with Google again (your key ended)")
    sub.add_parser("logout", help="log out")

    a = sub.add_parser("attach", help="connect a machine")
    a.add_argument("name")
    a.add_argument("--type", dest="kind", metavar="vm|container|kubernetes|local")
    a.add_argument("--runtime", metavar="containerd|systemd", help="VM only: where its jobs run")
    a.add_argument("--print", dest="print_only", action="store_true", help="print the lines, do not wait")
    a.add_argument("--ssh", metavar="USER@HOST", help="run the lines over ssh")
    a.add_argument("--key", metavar="FILE", help="ssh key")
    a.add_argument("--no-test", dest="test", action="store_false", help="skip the test run")

    tg = sub.add_parser("targets", help="where you can run")
    tg.add_argument("--json", action="store_true")

    i = sub.add_parser("install", help="packages every function on a machine sees")
    i.add_argument("name")
    i.add_argument("packages", nargs="*", metavar="PACKAGE")
    i.add_argument("-r", "--requirements", metavar="FILE", help="a requirements.txt")

    st = sub.add_parser("store", help="your bucket, where calls and results go")
    st.add_argument("uri", nargs="?", metavar="s3://BUCKET/PREFIX|gs://BUCKET/PREFIX", help="keep it with your login (no URI: show it)")
    st.add_argument("--forget", action="store_true", help="keep no bucket")

    d = sub.add_parser("detach", help="disconnect a machine")
    d.add_argument("name")

    r = sub.add_parser("run", help="run a command (a .py file: with Python)")
    r.add_argument("--on", metavar="NAME", help="a name from zenthros targets; none: this computer")
    r.add_argument("--snapshot", action="store_true", help="the whole git worktree, as it is now")
    r.add_argument("--detach", action="store_true", help="do not wait: it goes on without this computer")
    r.add_argument("--dry-run", action="store_true", help="say what would happen; nothing is created")
    r.add_argument("--timeout", metavar="TIME", help='it ends the job: "30m", "2h"')
    r.add_argument("--name")
    r.add_argument("--keep", action="store_true", help="keep the machine made for it")
    r.add_argument("--fresh", action="store_true", help="a new machine no job ran on, not one with nothing to do")
    r.add_argument("--code-dir", default=".", metavar="DIR")
    r.add_argument("--image", default="")
    r.add_argument("--env", default="", metavar="NAME", help="an environment from zenthros env list")
    r.add_argument("--cpu")
    r.add_argument("--memory")
    r.add_argument("--gpu", type=int)
    r.add_argument("--json", action="store_true")
    r.add_argument("command", nargs=argparse.REMAINDER, metavar="-- COMMAND")

    j = sub.add_parser("jobs", help="what ran and what runs")
    j.add_argument("--limit", type=int, default=50)
    j.add_argument("--json", action="store_true")
    s = sub.add_parser("status", help="one job")
    s.add_argument("job")
    s.add_argument("--json", action="store_true")
    g = sub.add_parser("logs", help="a job's log")
    g.add_argument("job")
    g.add_argument("--tail", type=int, default=2000)
    g.add_argument("--grep", metavar="REGEX")
    g.add_argument("--limit", type=int, default=2000)
    g.add_argument("--json", action="store_true")
    c = sub.add_parser("cancel", help="stop a job")
    c.add_argument("job")
    c.add_argument("--json", action="store_true")

    cl = sub.add_parser("clean", help="this project's local containers; what you name besides")
    cl.add_argument("--results", action="store_true", help="also what jobs past their keep_for left, but for their outputs")
    cl.add_argument("--outputs", action="store_true", help="with --results, their outputs too")
    cl.add_argument("--envs", action="store_true", help="also the cached Python of images no container has")
    cl.add_argument("--k8s", action="store_true", help="also the agent on-local-k8s installed on the cluster, with its namespace")
    cl.add_argument("--images", action="store_true", help="also the images zenthros env build made here, and the builder and its cache")
    cl.add_argument("--cache", action="store_true", help="also the cache entries older than --older")
    cl.add_argument("--older", default="180d", help="how old an entry must be to go (--cache): 180d, the default")
    cl.add_argument("--store", metavar="AGE", help="also what your bucket's jobs left, older than AGE; keeps what a "
                                                    "live job or a good cache entry needs")
    cl.add_argument("--dry-run", action="store_true", help="say what would go; nothing is removed")
    cl.add_argument("--json", action="store_true")
    _env_parser(sub)
    _cache_parser(sub)
    sub.add_parser("mcp", help="serve an agent over MCP, on stdin and stdout")
    ag = sub.add_parser("agent", help="what your agents may spend").add_subparsers(dest="agent_cmd", metavar="<command>", required=True)
    al = ag.add_parser("allow", help="let them use a target that costs money")
    al.add_argument("target")
    al.add_argument("--max-machines", type=int, help="at most this many at a time")
    de = ag.add_parser("deny", help="take that back")
    de.add_argument("target")
    sk = ag.add_parser("skill", help="the file that tells your agents how to use Zenthros")
    sk.add_argument("--to", default=".claude/skills", metavar="FOLDER", help="where agents look for skills; default: this project's")
    u = sub.add_parser("ui", help="what --json says, on a page, for this computer")
    u.add_argument("--port", type=int, default=0, help="default: any free one")
    u.add_argument("--no-open", dest="open", action="store_false", help="print the address, do not open a browser")
    return p


def _show(args, answer, text, out):
    import json

    print(json.dumps(answer) if args.json else text, file=out)


def _run(args, out):
    """The exit code: the process's; 125 if Zenthros could not start it."""
    from zenthros import tools

    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        print("✗ run what? zenthros run --on NAME -- COMMAND", file=out)
        return 2
    if command[0].endswith(".py"):
        from zenthros.remote.options import settings

        command = [settings().python, *command]
    try:
        answer = tools.run(command, on=args.on, snapshot=args.snapshot, timeout=args.timeout, keep=args.keep, fresh=args.fresh,
                           dry_run=args.dry_run, name=args.name, code_dir=args.code_dir, image=args.image, cpu=args.cpu,
                           memory=args.memory, gpu=args.gpu, env=args.env)
    except (ValueError, LookupError, PermissionError, RuntimeError, TimeoutError, ConnectionError) as exc:
        print(f"✗ {str(exc).strip() or type(exc).__name__}", file=out)
        return 125
    if args.dry_run or args.detach:
        text = str(answer) if args.dry_run else f"{answer['job']} · on {answer['on']}: zenthros status {answer['job']}"
        _show(args, answer, text, out)
        return 0
    import zenthros

    try:
        zenthros.get(answer["job"]).result()
    except errors.ZenthrosError:
        pass
    ended = tools.status(answer["job"])
    if args.json:
        _show(args, ended, "", out)
    return tools.exit_code_of(ended)


def _env_parser(sub):
    commands = sub.add_parser("env", help="environments: your packages in an image, kept and published").add_subparsers(
        dest="env_cmd", metavar="<command>", required=True)
    n = commands.add_parser("new", help="an environment from an image")
    n.add_argument("name")
    n.add_argument("--from", dest="base", required=True, metavar="IMAGE")
    n.add_argument("--registry", default="", metavar="REGISTRY", help="where zenthros env push sends it")
    a = commands.add_parser("add", help="packages for the next build")
    a.add_argument("name")
    a.add_argument("packages", nargs="*")
    a.add_argument("-r", dest="requirements", metavar="FILE")
    a.add_argument("--upgrade", action="store_true", help="let the versions in the lock move")
    b = commands.add_parser("build", help="build it here; nothing happens if nothing changed")
    b.add_argument("name")
    b.add_argument("--force", action="store_true", help="build it anyway, and take what comes out as the environment")
    u = commands.add_parser("use", help="local calls run in it, and work with no network")
    u.add_argument("name", nargs="?")
    u.add_argument("--off", action="store_true", help="back to the default image")
    p = commands.add_parser("push", help="publish what you tried")
    p.add_argument("name")
    p.add_argument("dest", nargs="?", metavar="REGISTRY/NAME")
    p.add_argument("--zstd", action="store_true", help="compress it with zstd; check the destination reads it")
    p.add_argument("--platform", metavar="OS/ARCH", help="another architecture: built for it, other bytes, same versions")
    commands.add_parser("list", help="this project's environments")
    for command in (n, a, b, u, p, commands.choices["list"]):
        command.add_argument("--json", action="store_true")


def _run_env(args, out):
    from zenthros import env

    if args.env_cmd == "list":
        rows = env.to_rows(env.list())
        lines = [f"{'*' if row['in_use'] else ' '} {row['name']}  {row['base']}  {row['packages']} packages  "
                 f"python {row['python'] or '?'}  {row['content'][:19] or 'not built'}"
                 + (f"  → {row['published'][-1]}" if row["published"] else "") for row in rows]
        _show(args, {"environments": rows}, "\n".join(lines) or "no environments: zenthros env new NAME --from IMAGE", out)
    elif args.env_cmd == "new":
        found = env.new(args.name, args.base, registry=args.registry)
        _show(args, found.to_dict(), f"✓ {found.name}: {found.base}@{found.base_digest[:19]}…", out)
    elif args.env_cmd == "add":
        found = env.add(args.name, *args.packages, requirements=args.requirements, upgrade=args.upgrade)
        _show(args, found.to_dict(), f"✓ {found.name}: {', '.join(found.packages)}; zenthros env build {found.name}", out)
    elif args.env_cmd == "build":
        found = env.build(args.name, force=args.force)
        _show(args, found.to_dict(), f"✓ {found.name}: python {found.python or '?'} · {found.content}", out)
    elif args.env_cmd == "use":
        if args.off == bool(args.name):
            raise ValueError("zenthros env use NAME, or zenthros env use --off")
        found = env.use(None if args.off else args.name)
        text = f"✓ local calls run in {found.name}, and it works with no network" if found else "✓ the default image again"
        _show(args, found.to_dict() if found else {}, text, out)
    elif args.env_cmd == "push":
        entry = env.push(args.name, dest=args.dest, zstd=args.zstd, platform=args.platform,
                         out=sys.stderr if args.json else out)
        if args.json:
            _show(args, entry, "", out)


def run(args, out):
    import zenthros

    if args.cmd == "login":
        if args.key:
            ws = zenthros.login(key=sys.stdin.readline().strip() if args.key == "-" else args.key)
        elif args.server or args.google:
            ws = zenthros.login(server=args.server or "", ca_file=args.ca_file or "", google=True)
        else:
            ws = zenthros.login()
        print(f"✓ Logged in (workspace: {getattr(ws, 'name', ws)})", file=out)
    elif args.cmd == "logout":
        zenthros.logout()
        print("✓ Logged out", file=out)
    elif args.cmd == "attach":
        zenthros.attach(args.name, type=args.kind, runtime=args.runtime, host=args.ssh, key=args.key,
                        wait=not args.print_only, test=args.test, out=out)
        if args.print_only:
            print("Then check with: zenthros targets", file=out)
    elif args.cmd == "targets":
        if args.json:
            from zenthros import tools

            _show(args, tools.targets(), "", out)
        else:
            print(zenthros.targets(), file=out)
    elif args.cmd == "install":
        zenthros.install(args.name, requirements=args.requirements, packages=args.packages, out=out)
    elif args.cmd == "store":
        from zenthros import userstore

        if args.forget:
            userstore.set_store(None, out=out)
        elif args.uri:
            userstore.set_store(args.uri, out=out)
        else:
            kept, region = userstore.saved()
            shown = f"{kept} ({region})" if region else kept
            print(shown or "no bucket kept: zenthros store s3://your-bucket/zenthros (or gs://)", file=out)
    elif args.cmd == "detach":
        rows = zenthros.targets()
        try:
            zenthros.delete(args.name)
        except LookupError:
            raise errors.ValidationError(f"no machine named {args.name}: see zenthros targets") from None
        print(f"✓ {args.name} detached", file=out)
        # No target has it: a join no machine used yet, so nothing runs there.
        kind = rows[args.name].type if args.name in rows else None
        if kind in ("vm", "container"):
            print("Its agent stops. The machine itself is yours and keeps running.", file=out)
        elif kind == "k8s" and rows[args.name].detail.get("id"):
            release = f"zenthros-{rows[args.name].detail['id']}"
            print(f"Its agent stops. To take it off the cluster: helm uninstall {release} -n {release}", file=out)
    elif args.cmd == "run":
        args.exit = _run(args, out)
    elif args.cmd == "jobs":
        from zenthros import tools

        answer = tools.jobs(limit=args.limit)
        text = "\n".join(f"{row['job']}  {row['state']}" + (f"  exit {row['exit_code']}" if row["exit_code"] is not None else "")
                         for row in answer["jobs"]) or "no jobs"
        _show(args, answer, text, out)
    elif args.cmd == "status":
        from zenthros import tools

        answer = tools.status(args.job)
        _show(args, answer, "\n".join(f"{key}: {value}" for key, value in answer.items() if value not in (None, "", [])), out)
    elif args.cmd == "logs":
        from zenthros import tools

        answer = tools.logs(args.job, tail=args.tail, grep=args.grep, limit=args.limit)
        _show(args, answer, "\n".join(answer["lines"]), out)
    elif args.cmd == "cancel":
        from zenthros import tools

        answer = tools.cancel(args.job)
        _show(args, answer, f"✓ {args.job} cancelled", out)
    elif args.cmd == "clean":
        _clean(args, out)
    elif args.cmd == "env":
        _run_env(args, out)
    elif args.cmd == "cache":
        _run_cache(args, out)
    elif args.cmd == "ui":
        _ui(args, out)
    elif args.cmd == "mcp":
        from zenthros import mcp

        mcp.serve()
    elif args.cmd == "agent":
        from zenthros import agents

        if args.agent_cmd == "allow":
            agents.allow(args.target, max_machines=args.max_machines, out=out)
        elif args.agent_cmd == "skill":
            _skill(args, out)
        else:
            agents.deny(args.target, out=out)
    else:
        return False
    return True


def _cache_parser(sub):
    ca = sub.add_parser("cache", help="what ran and need not run again").add_subparsers(dest="cache_cmd", metavar="COMMAND",
                                                                                        required=True)
    li = ca.add_parser("ls", help="every entry, and whether it is good")
    li.add_argument("--json", action="store_true")
    sh = ca.add_parser("show", help="one entry: what makes it, and what it holds")
    sh.add_argument("key", help="the start of its key")
    sh.add_argument("--json", action="store_true")
    rm = ca.add_parser("rm", help="forget one entry; its bytes stay until a clean")
    rm.add_argument("key", help="the start of its key")
    rm.add_argument("--json", action="store_true")
    sy = ca.add_parser("sync", help="enter the jobs you sent with cache= and did not read since they ended")
    sy.add_argument("--json", action="store_true")
    ke = ca.add_parser("key", help="the key that signs your entries, to share them with another computer of yours")
    ke.add_argument("what", choices=["export", "import"])
    ke.add_argument("--json", action="store_true")


def _run_cache(args, out):
    from zenthros import cache

    if args.cache_cmd == "ls":
        rows = cache.ls()
        lines = [f"{row['key'][:12]}  {row['fn'] or '?':<20} {row['job'] or '?':<28} {row['at'] or '?'}" if row["valid"] else
                 f"{row['key'][:12]}  not good: {row['problem']}" for row in rows]
        _show(args, {"entries": rows}, "\n".join(lines) or "nothing in the cache", out)
    elif args.cache_cmd == "show":
        answer = cache.show(args.key)
        _show(args, answer, json.dumps(answer, indent=1), out)
    elif args.cache_cmd == "rm":
        removed = cache.rm(args.key)
        _show(args, {"removed": removed}, f"✓ forgot {', '.join(key[:12] for key in removed)}", out)
    elif args.cache_cmd == "sync":
        entered = cache.sync()
        _show(args, {"entered": entered}, f"✓ entered {', '.join(entered)}" if entered else "nothing new", out)
    elif args.what == "export":
        print(cache.export_key(), file=out)
    else:
        cache.import_key(sys.stdin.read())
        print("✓ this computer now signs and reads the entries of that key", file=out)


def _ui(args, out):
    from zenthros import ui

    server = ui.serve(args.port)
    print(f"zenthros ui · {server.url} · this computer only; Ctrl-C ends it", file=out, flush=True)
    if args.open:
        import webbrowser

        webbrowser.open(server.url)
    try:
        server.wait()
    finally:
        server.stop()


def _skill(args, out):
    from pathlib import Path

    from zenthros import ui

    path = Path(args.to) / "zenthros" / "SKILL.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(ui.skill_text())
    print(f"✓ {path}: your agents can read how to use Zenthros (and zenthros mcp, if you add it)", file=out)


def _clean(args, out):
    from zenthros.remote.options import local_client, local_k8s_client

    client = local_client()
    # The containers first: what they no longer hold can go after them.
    answer = {"containers": client.clean(dry_run=args.dry_run)}
    if args.results:
        answer["results"] = (client.clean_results(dry_run=args.dry_run, outputs=args.outputs) +
                             local_k8s_client().clean_results(dry_run=args.dry_run, outputs=args.outputs))
    if args.k8s:
        answer["k8s"] = local_k8s_client().uninstall(dry_run=args.dry_run)
    if args.envs:
        answer["envs"] = client.clean_envs(dry_run=args.dry_run)
    if args.images:
        from zenthros import env

        answer["images"] = env.clean_images(dry_run=args.dry_run)
    if args.cache:
        from zenthros import cache

        answer["cache"] = cache.clean(args.older, dry_run=args.dry_run)
    if args.store:
        from zenthros import view

        report = view.workspace().clean(args.store, dry_run=args.dry_run)
        answer["store"] = report.calls
        answer["kept_cached"] = report.kept_cached
    verb = "would go" if args.dry_run else "removed"
    lines = [f"{what} {verb}: {', '.join(names)}" for what, names in answer.items() if names]
    _show(args, answer, "\n".join(lines) or "nothing to clean", out)


def entry():
    """The zenthros command. A Windows console that is not UTF-8 (output to a
    file or a pipe) has no ✓ and no █ of a job's log: a sign it cannot show is
    replaced, never the end of the command (zenthros.console, for the
    library's lines in a person's own program)."""
    for stream in (sys.stdout, sys.stderr):
        stream.reconfigure(errors="replace")
    sys.exit(main())


def main(argv=None, out=None):
    out = out or sys.stdout
    p = parser()
    args = p.parse_args(argv)
    try:
        if not run(args, out):
            p.print_help(out)
            return 2
        return getattr(args, "exit", 0)
    except KeyboardInterrupt:
        print(file=out)
        return 130
    except (ValueError, LookupError, PermissionError, RuntimeError, TimeoutError, ConnectionError) as exc:
        if os.environ.get("ZENTHROS_LOG_LEVEL", "").upper() == "DEBUG":
            raise
        message = str(exc).strip().splitlines() or [type(exc).__name__]
        print(f"✗ {message[0]}", file=out)
        for line in message[1:]:
            print(f"  {line}", file=out)
        return 1
    return 0


if __name__ == "__main__":
    entry()
