from QuickProject.Commander import Commander
from . import *

app = Commander(executable_name)


def store_last_id(job_id):
    with open(".last_id", "w") as f:
        f.write(job_id)


def get_last_id():
    if not os.path.exists(".last_id"):
        return None
    with open(".last_id", "r") as f:
        return f.read().strip()


@app.command()
def cancel(job_id: str = get_last_id()):
    """
    取消任务

    :param job_id: 任务ID
    """
    external_exec("scancel {}".format(job_id))


@app.command()
def template(name: str):
    """
    生成sbatch模板

    :param name: 任务名称
    """
    with open(f"{name}.sbatch", "w") as f:
        print(
            f"""\
#!/bin/bash
#SBATCH -J {name}
#SBATCH -p v6_384
#SBATCH -N 1           # 节点
#SBATCH -n 1           # 进程
#SBATCH -c 1           # 线程
#SBATCH -o log/%j.log
#SBATCH -e log/%j.err

# your commands here!""",
            file=f,
        )
    QproDefaultConsole.print(
        QproInfoString, f'已生成模板文件: "{name}.sbatch"，请勿修改`-o` `-e`参数！'
    )
    if not os.path.exists("log") or not os.path.isdir("log"):
        os.mkdir("log")


def get_job_id(command_output: str):
    """
    从命令输出中提取job_id

    :param command_output: 命令输出
    :return: job_id
    """
    return command_output.split()[-1].strip()


@app.command()
def view(job_id: str = get_last_id(), show_status: bool = False):
    """
    查看日志

    :param job_id: 任务ID
    :param show_status: 是否显示任务状态
    """
    # 实时显示日志，并在任务结束后退出
    from rich.markdown import Markdown
    from subprocess import Popen, PIPE
    from threading import Thread
    from queue import Queue
    import time

    def my_print(line):
        line = line.strip()
        if line.startswith("__START__"):
            QproDefaultStatus(line.replace("__START__", "")).start()
        elif line.startswith("__STOP__"):
            QproDefaultStatus.stop()
        elif line.startswith("__SPLIT__"):
            QproDefaultConsole.print(
                Markdown("# " + line.replace("__SPLIT__", "").strip())
            )
        else:
            QproDefaultConsole.print(line)

    def _output_reader(proc, output):
        for line in iter(proc.stdout.readline, b""):
            output.put(line)
        proc.stdout.close()

    def _error_reader(proc, error):
        for line in iter(proc.stderr.readline, b""):
            error.put(line)
        proc.stderr.close()

    def _monitor_job_running(proc, job_id, info_stream=None):
        while True:
            st, ct = external_exec(f"squeue -j {job_id}", without_output=True)
            if st:
                break
            job_info = ct.split("\n")[1]
            if not job_info:
                break
            if info_stream:
                info_stream.put(job_info)
            time.sleep(1)
        QproDefaultConsole.print(QproInfoString, "任务已结束！")
        proc.kill()

    def output_error_printer(proc, output, error, live=None, layout=None):
        if live:
            layout_height = int(QproDefaultConsole.height * 4 / 5)
            history = []  # only save height num lines
            while proc.poll() is None:
                if not output.empty():
                    history.append(output.get().decode("utf-8").rstrip())
                    if len(history) > layout_height:
                        history.pop(0)
                if not error.empty():
                    history.append(error.get().decode("utf-8").rstrip())
                    if len(history) > layout_height:
                        history.pop(0)
                layout["output"].update("\n".join(history[-layout_height:]))
                live.refresh()
            while not output.empty() or not error.empty():
                if not output.empty():
                    history.append(output.get().decode("utf-8").rstrip())
                    if len(history) > layout_height:
                        history.pop(0)
                if not error.empty():
                    history.append(error.get().decode("utf-8").rstrip())
                    if len(history) > layout_height:
                        history.pop(0)
                layout["output"].update("\n".join(history[-layout_height:]))
                live.refresh()
        else:
            while proc.poll() is None:
                if not output.empty():
                    my_print(output.get().decode("utf-8"))
                if not error.empty():
                    my_print(error.get().decode("utf-8"))
            while not output.empty() or not error.empty():
                if not output.empty():
                    my_print(output.get().decode("utf-8"))
                if not error.empty():
                    my_print(error.get().decode("utf-8"))

    log_path = f"log/{job_id}.log"
    output = Queue()
    error = Queue()
    squeue_info = Queue()
    proc = Popen(["tail", "-f", log_path], stdout=PIPE, stderr=PIPE)
    Thread(target=_output_reader, args=(proc, output)).start()
    Thread(target=_error_reader, args=(proc, error)).start()
    if show_status:
        Thread(target=_monitor_job_running, args=(proc, job_id, squeue_info)).start()

        import time
        from rich.layout import Layout
        from rich.live import Live
        from rich.align import Align
        from QuickStart_Rhy.TuiTools.Table import qs_default_table

        layout = Layout()
        layout.split_column(
            Layout(name="squeue", ratio=1, minimum_size=5),
            Layout(name="output", ratio=4, minimum_size=10),
        )

        def squeue_generator(layout, proc, info_stream):
            while proc.poll() is None:
                if info_stream.empty():
                    time.sleep(1)
                    continue
                qinfo = info_stream.get()
                table = qs_default_table(
                    ["任务ID", "任务队列", "任务名称", "用户", "状态", "用时", "节点数目", "节点列表"],
                    title="任务队列\n",
                )
                table.add_row(*qinfo.strip().split())
                layout["squeue"].update(Align.center(table))
                time.sleep(1)

        Thread(target=squeue_generator, args=(layout, proc, squeue_info)).start()

        with Live(
            layout, console=QproDefaultConsole, auto_refresh=False, screen=True
        ) as live:
            output_error_printer(proc, output, error, live, layout)
    else:
        Thread(target=_monitor_job_running, args=(proc, job_id)).start()
        output_error_printer(proc, output, error)

    QproDefaultConsole.print(QproInfoString, f"任务已结束，日志文件: {log_path}")


@app.command()
def error(job_id: str = get_last_id()):
    """
    查看错误信息
    """
    QproDefaultConsole.print(f"log/{job_id}.err")


@app.command()
def submit(script_path: str):
    """
    提交任务并查看日志

    :param script_path: 脚本路径
    """
    import time

    _, ct = external_exec("sbatch {}".format(script_path), without_output=True)
    job_id = get_job_id(ct)
    QproDefaultConsole.print(QproInfoString, f"任务已提交，任务ID: {job_id}")
    store_last_id(job_id)
    QproDefaultStatus("正在等待日志文件生成...").start()
    while not os.path.exists(f"log/{job_id}.log"):
        time.sleep(0.1)
    QproDefaultStatus.stop()
    app.real_call("view", job_id, True)


@app.command()
def top():
    """
    查看任务状态
    """
    import time
    from rich.live import Live
    from rich.align import Align
    from QuickStart_Rhy.TuiTools.Table import qs_default_table

    with Live(console=QproDefaultConsole, auto_refresh=False) as live:
        while True:
            _, ct = external_exec("squeue", without_output=True)
            table = qs_default_table(
                ["任务ID", "任务队列", "任务名称", "用户", "状态", "用时", "节点数目", "节点列表"],
                title="任务队列\n",
            )
            for line in ct.split("\n")[1:]:
                if line.strip() == "":
                    continue
                line = line.split()
                table.add_row(*line[:8])
            live.update(Align.center(table))
            live.refresh()
            time.sleep(1)


def main():
    """
    注册为全局命令时, 默认采用main函数作为命令入口, 请勿将此函数用作它途.
    When registering as a global command, default to main function as the command entry, do not use it as another way.
    """
    app()


if __name__ == "__main__":
    main()
