import marimo

__generated_with = "0.9.31"
app = marimo.App(width="full")


@app.cell
def __():
    import marimo as mo
    import polars as pl
    from fyers_apiv3 import fyersModel
    import json
    import datetime as dt
    from dateutil.relativedelta import relativedelta
    import pyotp
    import os

    fd=pl.read_parquet(f'C:\\Users\\{os.getlogin()}\\OneDrive\\Python\\stockdata.parquet')
    with open(f"C:\\Users\\{os.getlogin()}\\Downloads\\fyers.json") as f:
        f=json.load(f) 

    sym="TCS"
    return (
        dt,
        f,
        fd,
        fyersModel,
        json,
        mo,
        os,
        pl,
        pyotp,
        relativedelta,
        sym,
    )


@app.cell
def __(fd, pl, sym):
    from pyecharts.charts import Bar, Candlestick, Kline
    from pyecharts import options as opts
    from pyecharts.globals import CurrentConfig, NotebookType
    CurrentConfig.NOTEBOOK_TYPE = NotebookType.ZEPPELIN

    df=fd.filter(pl.col('symbol')==sym).sort('epoch',descending=False)
    df=df.with_columns(gain=pl.when(pl.col('open')<pl.col('close')).then(1).otherwise(-1))
    bar = (
        Candlestick(init_opts=opts.InitOpts(height="600px",width="1320px"))
        .add_xaxis(df['epoch'].cast(pl.String).to_list())
        .add_yaxis(series_name="", y_axis=df['open','close','low','high'].to_numpy().tolist(),itemstyle_opts=opts.ItemStyleOpts(color="#00da3c",border_color="#00da3c", color0="#ec0000",border_color0="#ec0000"),)
        #.add_dataset(data=list(df['open','high','low','close','gain'].to_numpy().tolist()),)
        .set_global_opts(title_opts=opts.TitleOpts(title=f"{sym}"), datazoom_opts=[
                    opts.DataZoomOpts(is_show=False,type_="inside",xaxis_index=[0, 1],range_start=90,range_end=100,),
            opts.DataZoomOpts(is_show=True,xaxis_index=[0, 5],type_="slider",pos_bottom="5%",range_start=85,range_end=100,)],
                        tooltip_opts=opts.TooltipOpts(trigger="axis",axis_pointer_type="cross",background_color="rgba(245, 245, 245, 0.8)",border_width=1,border_color="#ccc",textstyle_opts=opts.TextStyleOpts(color="#000"),),)
    )
    bar.load_javascript()
    bar.render_notebook()
    return (
        Bar,
        Candlestick,
        CurrentConfig,
        Kline,
        NotebookType,
        bar,
        df,
        opts,
    )


@app.cell
def __(bar):
    bar
    return


if __name__ == "__main__":
    app.run()
