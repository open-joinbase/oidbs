import matplotlib.pyplot as plt
import pandas as pd
import csv
from pathlib import Path
import os
import sys


def process_results(file_name):
    dat_file = Path(__file__).with_name(file_name)
    cd = os.path.join(".", file_name)
    if Path(cd).exists():
        dat_file = Path(cd)
    # print(dat_file)
    df = pd.read_csv(dat_file.open('r'))
    index = list(df.columns)[1:]
    # print("index:"+', '.join(index))
    df = df.transpose()
    # print(df)

    dfn = pd.DataFrame()
    for (_, columnData) in df.iteritems():
        dfn[columnData.values[0]] = columnData.values[1:]

    dfn.index = index
    return dfn


def print_ratios_to_ib(df):
    col_ib = df['JoinBase']
    for (name, col) in df.iteritems():
        if name != 'JoinBase':
            r = df[name]/df['JoinBase']
            print(f'Time cost ratio - {name}:JoinBase\n{r}')


# if os.path.isdir(dir_data):
    # bench latency
df = process_results('latency_results.csv')
# print(df)
ax = df.plot.bar(rot=0, log=True, ec="k", color=['g', 'lightblue', 'orange'])
ax.axhline(1e3, color="k", ls='--')
ax.axhline(1e6, color="k", ls='--')
plt.title("OIDBS Benchmark - Query Latency")
plt.xlabel("Queries")
plt.ylabel("End-to-end Query Time (log scale) (microseconds)")
# plt.show()
plt.savefig('oidbs_bench_latency_results.png', bbox_inches='tight')
print_ratios_to_ib(df)

# bench latency
df = process_results('concurrency_results.csv')
ax = df.plot.bar(rot=0, log=True, ec="k", color=['g', 'lightblue', 'orange'])
ax.axhline(1e2, color="k", ls='--')
ax.axhline(1e3, color="k", ls='--')
ax.axhline(1e4, color="k", ls='--')
plt.title("OIDBS Benchmark - Query Concurrency")
plt.xlabel("Queries")
plt.ylabel("Number of Queries Per Second (log scale)")
# plt.show()
plt.savefig('oidbs_bench_concurrency_results.png', bbox_inches='tight')
