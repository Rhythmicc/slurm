from QuickProject.Commander import Commander
from . import *

app = Commander(executable_name)


@app.command()
def view(log_path: str):
    """
    查看任务日志

    :param log_path: 日志路径
    """
    external_exec('tail -f {}'.format(log_path))


@app.command()
def cancel(job_id: str):
    """
    取消任务

    :param job_id: 任务ID
    """
    external_exec('scancel {}'.format(job_id))


@app.command()
def template(name: str):
    """
    生成sbatch模板

    :param name: 任务名称
    """
    with open(f'{name}.sbatch', 'w') as f:
        print(f"""\
#!/bin/bash
#SBATCH -J {name}
#SBATCH -p v6_384
#SBATCH -n 1
#SBATCH -c 1
#SBATCH -o log/%j.log
#SBATCH -e log/%j.err

# your commands here!""", file=f)
    QproDefaultConsole.print(QproInfoString, f'已生成模板文件: "{name}.sbatch"，请勿修改`-o` `-e`参数！')
    if not os.path.exists('log') or not os.path.isdir('log'):
        os.mkdir('log')

def get_job_id(command_output: str):
    """
    从命令输出中提取job_id

    :param command_output: 命令输出
    :return: job_id
    """
    return command_output.split()[-1].strip()

def view_log(log_path: str, job_id: str):
    """
    查看日志

    :param log_path: 日志路径
    :param job_id: 任务ID
    """
    # 实时显示日志，并在任务结束后退出
    from rich.markdown import Markdown
    from subprocess import Popen, PIPE
    from threading import Thread

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
        for line in iter(proc.stdout.readline, b''):
            output.append(line)
        proc.stdout.close()

    def _error_reader(proc, error):
        for line in iter(proc.stderr.readline, b''):
            error.append(line)
        proc.stderr.close()
    
    def _monitor_job_running(proc, job_id):
        import time
        while True:
            _, ct = external_exec(f'squeue -j {job_id}', without_output=True)
            if not ct.split('\n')[1:]:
                break
            time.sleep(1)
        proc.kill()
    
    output = []
    error = []
    proc = Popen(['tail', '-f', log_path], stdout=PIPE, stderr=PIPE)
    Thread(target=_output_reader, args=(proc, output)).start()
    Thread(target=_error_reader, args=(proc, error)).start()
    Thread(target=_monitor_job_running, args=(proc, job_id)).start()
    while proc.poll() is None:
        if output:
            my_print(output.pop().decode('utf-8'))
        if error:
            my_print(error.pop().decode('utf-8'))
    
    # 任务结束后，显示最后的日志
    while output:
        my_print(output.pop().decode('utf-8'))
    while error:
        my_print(error.pop().decode('utf-8'))
    QproDefaultConsole.print(QproInfoString, f'任务已结束，日志文件: {log_path}')


@app.command()
def submit(script_path: str):
    """
    提交任务并查看日志

    :param script_path: 脚本路径
    """
    import time

    _, ct = external_exec('sbatch {}'.format(script_path), without_output=True)
    job_id = get_job_id(ct)
    QproDefaultConsole.print(QproInfoString, f'任务已提交，任务ID: {job_id}')
    QproDefaultStatus('正在等待日志文件生成...').start()
    while not os.path.exists(f'log/{job_id}.log'):
        time.sleep(0.1)
    QproDefaultStatus.stop()
    view_log(f'log/{job_id}.log', job_id)


@app.command()
def top():
    """
    查看任务状态
    """
    import time
    from rich.live import Live
    from rich.align import Align
    from QuickStart_Rhy.TuiTools.Table import qs_default_table
    table = qs_default_table(
        ['任务ID', '任务队列', '任务名称', '用户', '状态', '用时', '节点数目', '节点列表'], title='任务队列\n')

    with Live(console=QproDefaultConsole, auto_refresh=False) as live:
        while True:
            _, ct = external_exec('squeue', without_output=True)
            table = qs_default_table(['任务ID', '任务队列', '任务名称', '用户', '状态', '用时', '节点数目', '节点列表'], title='任务队列\n')
            for line in ct.split('\n')[1:]:
                if line.strip() == '':
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
