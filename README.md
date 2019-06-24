# jupyter_hive
A module to help interaction with Jupyter Notebooks and Apache Hive


###
This is a python module that helps to connect Jupyter Notebooks to Apache Hive. It uses the requests module to make request via the Hiveserver2  and brings back the data as a data frame


After installing this, to instantiate the module so you can use %hive and %%hive put this in a cell:

```
from hive_core import Hive
ipy = get_ipython()
Hive = Hive(ipy,  pd_use_beaker=True)
ipy.register_magics(Hive)
```
