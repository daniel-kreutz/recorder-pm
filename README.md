recorder-viz
=============

This is a python package for calculating performance metrics with [Recorder](https://github.com/uiuc-hpc/Recorder) traces.

Installation
-------------

`recorder-pm` relies on Recorder to run. It can be installed via pip with
```shell
pip install --user git+https://github.com/daniel-kreutz/recorder-pm.git
```


Usage
-------------

```shell
recorder-metrics -i=path/to/trace -o=path/to/report
```
