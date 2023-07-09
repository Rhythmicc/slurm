<h1 style="text-align: center"> slurm </h1>

## Install

```shell
pip3 install slurm -U
```

## Usage

```shell
slurm --help
```

## Developer

If you need use global config, just edit `__config__.py`:

1. make `enable_config = True`.
2. edit `questions` list.
3. using `config` at `main.py`.
