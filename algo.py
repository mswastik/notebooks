import marimo

__generated_with = "0.10.5"
app = marimo.App(width="full")


@app.cell(hide_code=True)
def _():
    import marimo as mo
    import polars as pl
    from fyers_apiv3 import fyersModel
    import json
    import datetime as dt
    from dateutil.relativedelta import relativedelta
    import pyotp
    import os
    import webbrowser
    import time
    import pyperclip
    from urllib.parse import parse_qs,urlparse
    import pickle
    from pyecharts.charts import Bar, Candlestick, Kline
    from pyecharts import options as opts
    from pyecharts.globals import CurrentConfig, NotebookType
    CurrentConfig.NOTEBOOK_TYPE = NotebookType.JUPYTER_LAB

    fd=pl.read_parquet(f'C:\\Users\\{os.getlogin()}\\OneDrive\\Python\\stockdata.parquet')
    #fd=fd.with_columns(pl.col('open','high','low','close').round(3))
    with open(f"C:\\Users\\{os.getlogin()}\\Downloads\\fyers.json") as f:
        f=json.load(f)

    class A():
        fyers=''
        dd=''
    a=A()

    pyotp.TOTP(f['totp']).now()
    return (
        A,
        Bar,
        Candlestick,
        CurrentConfig,
        Kline,
        NotebookType,
        a,
        dt,
        f,
        fd,
        fyersModel,
        json,
        mo,
        opts,
        os,
        parse_qs,
        pickle,
        pl,
        pyotp,
        pyperclip,
        relativedelta,
        time,
        urlparse,
        webbrowser,
    )


@app.cell(hide_code=True)
def _(
    a,
    f,
    fyers,
    fyersModel,
    name,
    parse_qs,
    pickle,
    pyperclip,
    time,
    urlparse,
    webbrowser,
):
    def generate_access_token():
        global user_id, pin, app_id, api_key, redirect_url, totp_key, url
        print("\nGenerating Access Token .....................")
        try:
            user_id = f["ci"]
            pin = f["pin"]
            app_id = f["client_id"]
            api_key = f["secret_key"]
            redirect_url = f["redirect_uri"]
            totp_key = f["totp"]
            session = fyersModel.SessionModel(
                client_id=app_id,
                secret_key=api_key,
                redirect_uri=redirect_url,
                response_type="code",
                state=f["state"],
            )
            url = session.generate_authcode()
            webbrowser.open_new(url)
            print("Copy URL to clipboard")
            old_clipboard_contents = "a"
            new_clipboard_contents = "a"
            while old_clipboard_contents == new_clipboard_contents:
                time.sleep(5)
                new_clipboard_contents = pyperclip.paste()
            url = new_clipboard_contents
            parsed = urlparse(url)
            auth_code = parse_qs(parsed.query)["auth_code"][0]
            session = fyersModel.SessionModel(
                client_id=app_id,
                secret_key=api_key,
                redirect_uri=redirect_url,
                response_type="code",
                grant_type="authorization_code",
            )
            session.set_token(auth_code)
            response = session.generate_token()
            access_token = response["access_token"]
            refresh_token = response["refresh_token"]
            token_dict = {
                "app_id": app_id,
                "access_token": access_token,
                "refresh_token": refresh_token,
            }
            with open("token_dict.pickle", "wb") as file:
                pickle.dump(token_dict, file)
        except:
            time.sleep(5)
            generate_access_token()


    def fyers_login(e):
        global fyers, token_dict, name
        while True:
            try:
                with open("token_dict.pickle", "rb") as file:
                    token_dict = pickle.load(file)
                    print(token_dict)
            except:
                token_dict = {"app_id": 0, "access_token": 0, "refresh_token": 0}
            a.fyers = fyersModel.FyersModel(
                client_id=token_dict["app_id"],
                is_async=False,
                token=token_dict["access_token"],
                log_path="",
            )
            response = a.fyers.get_profile()
            if response["s"] == "error":
                generate_access_token()
            else:
                print("\nlogin Details ..........")
                print(a.fyers.get_profile()["data"]["name"])
                # print(name)
                break

    #mo.ui.button(on_click=fyers_login, label="Login")
    return (
        api_key,
        app_id,
        fyers_login,
        generate_access_token,
        pin,
        redirect_url,
        token_dict,
        totp_key,
        url,
        user_id,
    )


@app.cell
def _(a, dt, fd, mo, os, pl, relativedelta):
    sym = mo.ui.dropdown(fd["symbol"].unique().to_list())
    PARQUET_FILE = f'C:\\Users\\{os.getlogin()}\\OneDrive\\Python\\stockdata1.parquet'
    def fetch_data(ticker, sd, ed):
        std=a.fyers.history(data={'symbol':f'NSE:{ticker}-EQ','resolution':'1D','date_format':1,'range_from':sd,'range_to':ed,'cont_flag':1})
        df1=pl.DataFrame(std['candles'],schema={'epoch':pl.Int32,'open':pl.Float32,'high':pl.Float32,'low':pl.Float32,'close':pl.Float32,'volume':pl.Float32},orient="row")
        df1=df1.with_columns(epoch=pl.from_epoch('epoch'))
        df1=df1.with_columns(symbol=pl.lit(ticker))
        return df1

    def save_to_parquet(data, file_name):
        if os.path.exists(file_name):
            existing_data = pl.read_parquet(file_name)
            data = pl.concat([existing_data, data]).unique(subset=["epoch", "Symbol"])
            #data = data[~data.index.duplicated(keep='last')]  # Remove duplicates
        data.write_parquet(file_name)
        print(f"Data saved to {file_name}")

    def update_parquet_data(ticker):
        ticker=sym.value
        if os.path.exists(PARQUET_FILE):
            # Load existing data and find the last available date for the ticker
            all_data = pl.read_parquet(PARQUET_FILE)
            if ticker in all_data['Symbol'].values:
                ticker_data = all_data.filter(pl.col('Symbol') == ticker)
                last_date = ticker_data.select(pl.col('epoch').max()).to_numpy()[0,0]
                start_date= (last_date+dt.timedelta(hours=24)).strftime('%Y-%m-%d')
                #start_date = (last_date + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
            else:
                start_date = '2020-01-01'
        else:
            start_date = '2020-01-01'
        end_date = (dt.datetime.now()).strftime('%Y-%m-%d')
        new_data = fetch_data(ticker, start_date, end_date)

        if not new_data.empty:
            save_to_parquet(new_data, PARQUET_FILE)
        else:
            print(f"No new data available for {ticker}.")

    def getd(e):
        ny=5
        try:
            print('try')
            sd=fd.filter(pl.col('symbol')==sym.value)['epoch'].max().strftime('%Y-%m-%d')
            ed=(dt.datetime.now()-dt.timedelta(hours=24)).strftime('%Y-%m-%d')
            std=a.fyers.history(data={'symbol':f'NSE:{sym.value}-EQ','resolution':'1D','date_format':1,'range_from':sd,'range_to':ed,'cont_flag':1})
            df1=pl.DataFrame(std['candles'],schema={'epoch':pl.Int32,'open':pl.Float32,'high':pl.Float32,'low':pl.Float32,'close':pl.Float32,'volume':pl.Float32},orient="row")
            df1=df1.with_columns(epoch=pl.from_epoch('epoch'))
            df1=df1.with_columns(symbol=pl.lit(sym.value))
            fd1=pl.concat([fd,df1])
            fd1.write_parquet(f'C:\\Users\\{os.getlogin()}\\OneDrive\\Python\\stockdata.parquet')
        except:
            print('except')
            for i in range(ny):
                print('loop')
                ed=(dt.datetime.now()-dt.timedelta(hours=24)-relativedelta(years=i)).strftime('%Y-%m-%d')
                sd=(dt.datetime.now()-dt.timedelta(hours=24)-relativedelta(years=i+1)).strftime('%Y-%m-%d')
                std=a.fyers.history(data={'symbol':f'NSE:{sym.value}-EQ','resolution':'1D','date_format':1,'range_from':sd,'range_to':ed,'cont_flag':1})
                df1=pl.DataFrame(std['candles'],schema={'epoch':pl.Int32,'open':pl.Float32,'high':pl.Float32,'low':pl.Float32,'close':pl.Float32,'volume':pl.Float32},orient="row")
                df1=df1.with_columns(epoch=pl.from_epoch('epoch'))
                df1=df1.with_columns(symbol=pl.lit(sym.value))
                fd1=pl.concat([fd,df1])
            fd1.write_parquet(f'C:\\Users\\{os.getlogin()}\\OneDrive\\Python\\stockdata.parquet')
    #mo.ui.button(on_click=getd, label='Update Data')
    return (
        PARQUET_FILE,
        fetch_data,
        getd,
        save_to_parquet,
        sym,
        update_parquet_data,
    )


@app.cell
def _(fyers_login, mo, sym, update_parquet_data):
    mo.hstack([sym,mo.ui.button(on_click=fyers_login, label="Login"),mo.ui.button(on_click=update_parquet_data, label='Update Data')], justify='start',gap=2)
    return


@app.cell(hide_code=True)
def _(Candlestick, fd, opts, pl, sym):
    df=fd.filter(pl.col('symbol')==sym.value).sort('epoch',descending=False)
    df=df.with_columns(gain=pl.when(pl.col('open')<pl.col('close')).then(1).otherwise(-1))
    bar = (
        Candlestick(init_opts=opts.InitOpts(height="600px",width="1320px"))
        .add_xaxis(df['epoch'].cast(pl.String).to_list())
        .add_yaxis(series_name="", y_axis=df['open','close','low','high'].to_numpy().tolist(),itemstyle_opts=opts.ItemStyleOpts(color="#00da3c",border_color="#00da3c", color0="#ec0000",border_color0="#ec0000"),)
        #.add_dataset(data=list(df['open','high','low','close','gain'].to_numpy().tolist()),)
        .set_global_opts(title_opts=opts.TitleOpts(title=f"{sym.value}"), datazoom_opts=[
                    opts.DataZoomOpts(is_show=False,type_="inside",xaxis_index=[0, 1],range_start=90,range_end=100,),
            opts.DataZoomOpts(is_show=True,xaxis_index=[0, 5],type_="slider",pos_bottom="5%",range_start=85,range_end=100,)],
                        tooltip_opts=opts.TooltipOpts(trigger="axis",axis_pointer_type="cross",background_color="rgba(245, 245, 245, 0.8)",border_width=1,border_color="#ccc",textstyle_opts=opts.TextStyleOpts(color="#000"),),)
    )
    bar.load_javascript()
    bar.render_notebook()
    bar
    return bar, df


@app.cell
def _(a, dt, fd, pl, sym):
    import talib as tl
    from backtesting import Backtest, Strategy
    from backtesting.lib import crossover
    import numpy as np
    import pandas as pd
    #dd=pd.DataFrame()

    def ta_MACD(df):
        ""
        return tl.MACD(df,fastperiod=10,slowperiod=20,signalperiod=6)


    class MACD(Strategy):
        def init(self):
            price = self.data['Close']
            self.macd, self.sig, self.hist =self.I(ta_MACD, df1['Close'])
            #self.sma2 = tl.SMA(df['Close'],timeperiod=60)
            a.dd=pd.DataFrame({'macd':self.macd,'sig':self.sig,'hist':self.hist})
        def next(self):
            if crossover(self.macd, self.sig):
                self.buy()
            elif crossover(self.sig, self.macd):
                try:
                    #print(self.closed_trades[-1].entry_price)
                    pinc=(self.data.Close[-1]-self.closed_trades[-1].entry_price)/self.closed_trades[-1].entry_price
                    if pinc>.25:
                        #self.sell()
                        self.position.close()
                except:
                    pass
    df1=fd.filter(pl.col('symbol')==sym.value).filter(pl.col('epoch')>=dt.date(2021,9,9)).drop('symbol').sort('epoch',descending=False)
    df1=df1.rename({'open':"Open",'high':'High','low':'Low','close':"Close",'volume':'Volume'})
    #df1=df1.with_columns(pl.col('epoch').cast(pl.String))
    df1=df1.to_pandas()
    #df1['epoch']=pd.to_datetime(df1['epoch'])
    df1.set_index('epoch',inplace=True)
    #df1.reset_index(inplace=True)
    bt = Backtest(df1, MACD,cash= 100000,exclusive_orders=True)
    stats = bt.run()
    bt.plot(plot_return=False,)
    return (
        Backtest,
        MACD,
        Strategy,
        bt,
        crossover,
        df1,
        np,
        pd,
        stats,
        ta_MACD,
        tl,
    )


@app.cell
def _(pd, stats):
    pd.DataFrame(stats,columns=['stat'])
    return


@app.cell
def _(a, opts):
    from pyecharts.charts import Line
    Line(init_opts=opts.InitOpts(height="600px",width="1320px")).add_xaxis(a.dd.index.tolist()).add_yaxis(series_name="", y_axis=list(a.dd['sig'].to_numpy()),label_opts=opts.LabelOpts(is_show=False)).add_yaxis(series_name="", y_axis=list(a.dd['macd'].to_numpy()),label_opts=opts.LabelOpts(is_show=False)).add_yaxis(series_name="", y_axis=list(a.dd['hist'].to_numpy()),label_opts=opts.LabelOpts(is_show=False))
    return (Line,)


if __name__ == "__main__":
    app.run()
