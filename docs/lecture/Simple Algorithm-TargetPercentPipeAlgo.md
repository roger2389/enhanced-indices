# Target Percent Pipeline Algorithm

利用pipeline提供的買賣清單與持股權重進行定期再平衡的演算法。

<span id="menu"></span>
本文件包含以下部份：

**基本設定**
1. [Set Environment Variables](#Env)
2. [Investment Universe](#Universe)
3. [Ingest](#Ingest)
4. [Imports](#Imports)
5. [Pipeline](#Pipeline)

**參數說明與範例**

6. [API Reference](#APIReference)：參數說明。
   - [class zipline.algo.pipeline_algo.TargetPercentPipeAlgo](#APIReference)
   - [class zipline.api.date_rules](#date_rules)


7. [Examples](#Examples)：範例。
   - [Case 1－調整start_session與end_session](#Case1)
   - [Case 2－調整max_leverage](#Case2)
   - [Case 3－調整tradeday](#Case3)
   - [Case 4－調整rebalance_date_rule](#Case4)
   - [Case 5－調整slippage_model](#Case5)
   - [Case 6－調整stocklist](#Case6)
   - [Case 7－調整order_filling_policy](#Case7)
   - [Case 8－調整allow_short](#Case8)
   - [Case 9－調整cancel_datedelta](#Case9)   
   - [Case 10－調整limit_buy_multiplier](#Case10)  
   - [Case 11－調整custom_weight、analyze、record_vars、get_record_vars與get_transaction_detail](#Case11)   

<span id="Env"></span>
# 1. Set Environment Variables
[Return to Menu](#menu)


```python
import pandas as pd
import datetime
import tejapi
import os

# set tej_key and base
os.environ['TEJAPI_KEY'] = "your key" 
os.environ['TEJAPI_BASE'] = "https://api.tej.com.tw"

# set benchmark
benchmark=['IR0001']

# set calendar
calendar_name='TEJ_XTAI'

# set bundle name
bundle_name = 'tquant'

# set date
start='2023-06-01'
end='2023-10-03'

# 由文字型態轉為Timestamp，供回測使用
tz = 'UTC'
start_dt, end_dt = pd.Timestamp(start, tz = tz), pd.Timestamp(end, tz = tz)

# 設定os.environ['mdate'] = start+' '+end，供ingest bundle使用
os.environ['mdate'] = start+' '+end
```

<span id="Universe"></span>
# 2. Investment Universe

台灣50指數成分股

[Return to Menu](#menu)


```python
from zipline.sources.TEJ_Api_Data import get_universe

StockList = get_universe(start, end, idx_id='IX0002')

print(len(StockList))
```

    Currently used TEJ API key call quota 67/9223372036854775807 (0.0%)
    Currently used TEJ API key data quota 2255014/9223372036854775807 (0.0%)
    55
    


```python
os.environ['ticker'] = ' '.join(StockList + benchmark)
```


```python
os.environ['ticker']
```




    '1101 1216 1301 1303 1326 1402 1590 1605 2002 2207 2301 2303 2308 2317 2327 2330 2345 2357 2379 2382 2395 2408 2412 2454 2603 2609 2615 2801 2880 2881 2882 2883 2884 2885 2886 2887 2890 2891 2892 2912 3008 3034 3037 3045 3231 3711 4904 4938 5871 5876 5880 6415 6505 6669 9910 IR0001'



<span id="Ingest"></span>
# 3. Ingest
[Return to Menu](#menu)


```python
# Ingest pricing bundle
!zipline ingest -b tquant
```

    Merging daily equity files:
    Currently used TEJ API key call quota 72/9223372036854775807 (0.0%)
    Currently used TEJ API key data quota 2261479/9223372036854775807 (0.0%)
    

    [2024-03-13 02:40:44.060855] INFO: zipline.data.bundles.core: Ingesting tquant.
    [2024-03-13 02:40:49.764291] INFO: zipline.data.bundles.core: Ingest tquant successfully.
    


```python
from zipline.data.data_portal import get_bundle_price

df_bundle_price = get_bundle_price(bundle_name=bundle_name,
                                   calendar_name=calendar_name,
                                   start_dt=start_dt,
                                   end_dt=end_dt)
```

<span id="Imports"></span>
# 4. Imports & Settings
[Return to Menu](#menu)


```python
import warnings
warnings.filterwarnings('ignore')
```


```python
from time import time
import numpy as np
import pandas as pd
import empyrical as ep

from logbook import Logger, StderrHandler, INFO, WARNING
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator


from TejToolAPI.TejToolAPI import *

from zipline.api import record

from zipline.utils.calendar_utils import get_calendar
from zipline.utils.run_algo import  (get_transaction_detail,
                                     get_record_vars)

from zipline.pipeline import Pipeline, CustomFactor
from zipline.pipeline.filters import SingleAsset
from zipline.pipeline.factors import RSI
from zipline.pipeline.data import TQAltDataSet

from zipline.finance import slippage

from zipline.TQresearch.tej_pipeline import run_pipeline

# 設定log顯示方式
log_handler = StderrHandler(format_string='[{record.time:%Y-%m-%d %H:%M:%S.%f}]: ' +
                            '{record.level_name}: {record.func_name}: {record.message}',
                            level=INFO)
log_handler.push_application()
log = Logger('Algorithm')
```


```python
import warnings
warnings.filterwarnings('ignore')
```

<span id="Pipeline"></span>
# 5. Pipeline

取得`Market_Cap_Dollars`（市值）資料

[Return to Menu](#menu)


```python
col = ['Market_Cap_Dollars']

fields = ''
for i in col:
    fields += i
    fields += ' '

os.environ['fields'] = fields
```


```python
# Ingest Fundamental Bundle
!zipline ingest -b fundamentals
```

    Currently used TEJ API key call quota 83/9223372036854775807 (0.0%)
    Currently used TEJ API key data quota 2298569/9223372036854775807 (0.0%)
    

    [2024-03-13 02:40:54.272410] INFO: zipline.data.bundles.core: Ingesting fundamentals.
    [2024-03-13 02:41:05.038363] INFO: zipline.data.bundles.core: Ingest fundamentals successfully.
    

<span id="APIReference"></span>
# 6. API Reference
[Return to Menu](#menu)

### **class zipline.algo.pipeline_algo.<font color=DeepPink>TargetPercentPipeAlgo</font>** 
*(self, bundle_name='tquant', start_session=None, end_session=None, trading_calendar=get_calendar('TEJ_XTAI'),*

*capital_base=1e7, data_frequency='daily', tradeday=None, rebalance_date_rule=None, stocklist=None, benchmark='IR0001',* 

*zero_treasury_returns=True, slippage_model=slippage.VolumeShareSlippage(volume_limit=0.025, price_impact=0.1),*

*commission_model=commission.Custom_TW_Commission(min_trade_cost=20, discount = 1.0, tax = 0.003), max_leverage=0.8,* 

*limit_buy_multiplier=None, limit_sell_multiplier=None, allow_short=False, cancel_datedelta=None, custom_weight=False,*

*custom_loader=None, pipeline=None, analyze=None, record_vars=None, get_record_vars=False, get_transaction_detail=False,*

*order_filling_policy='next_bar')*

利用pipeline提供的買賣清單與持股權重進行定期再平衡的演算法。必要參數僅有`pipeline`。


#### Parameters:

- **bundle_name** (*str, optional*)－bundle名稱。預設是 **`'tquant'`**。  


- **start_session** (*pd.Timestamp or datetime, optional*)－回測起始日期。預設是**bundle中最早的資料日期**。  


- **end_session** (*pd.Timestamp or datetime, optional*)－回測結束日期。預設是**bundle中最晚的資料日期**。  


- **trading_calendar** (*TradingCalendar, optional*)－
  - 設置交易日曆。預設是 **`get_calendar('TEJ_XTAI')`**。
  - TradingCalendar：`zipline.utils.calendar_utils.TradingCalendar`。  


- **capital_base** (*float, optional*)－初始資金額度。預設是**一千萬**。  


- **data_frequency** (*{'daily', 'minute'}, optional*)－資料頻率，目前僅支援日頻率 **`'daily'`**。  


- **tradeday** (*list[str] or list[pd.Timestamp] or pd.DatetimeIndex, optional*)－
  - 交易日期清單，限制只能在這個清單中的日期進行交易。預設是**None**，代表**每日**都交易。
  - `rebalance_date_rule`與`tradeday`請擇一設定，若兩者皆設定，則會以`rebalance_date_rule`為主。  


- **rebalance_date_rule**(*EventRule, optional*)－
  - 交易頻率，設定固定的交易頻率。預設是**None**，代表**每日**都交易。
  - `rebalance_date_rule`與`tradeday`請擇一設定，若兩者皆設定，則會以`rebalance_date_rule`為主。
  - EventRule：`zipline.utils.events.EventRule`。可使用的`rebalance_date_rule`參考[zipline.api.date_rules](#date_rules)。  


- **stocklist** (*list[str], optional*)－交易清單，限制只能交易這個清單中的股票。預設是**None**，代表使用所有bundle中的股票。  


- **benchmark** (*str, optional*)－指數名稱，用來與投資組合報酬率比較。預設是`'IR0001'`，代表**台灣發行量加權股價報酬指數**。  


- **zero_treasury_returns** (*bool, optional*)－
  - 是否將無風險利率設定為0，預設是**True**，代表設定為0。因此使用`algo.run()`產出的回測報表中，`treasury_return`與`treasury_period_return`皆會是0。
  - 若設定為**False**，則會<font color=DeepPink>耗費額外API流量</font>，並取得第一銀行一年期定存利率作為無風險利率。  


- **slippage_model** (*SlippageModel, optional*)－
  - 設定滑價模型。預設為`slippage.VolumeShareSlippage(volume_limit=0.025, price_impact=0.1)`。可使用的模型參考：[lecture/Zipline Slippage.ipynb](https://github.com/tejtw/TQuant-Lab/blob/main/lecture/Zipline%20Slippage.ipynb)
  - SlippageModel：`zipline.finance.slippage.SlippageModel`。


- **commission_model** (*CommissionModel, optional*)－
  - 設定手續費模型。預設為`commission.Custom_TW_Commission(min_trade_cost=20, discount=1.0, tax = 0.003)`。可使用的模型參考：[lecture/Zipline Commission Models.ipynb](https://github.com/tejtw/TQuant-Lab/blob/main/lecture/Zipline%20Commission%20Models.ipynb)
  - CommissionModel：`zipline.finance.commission.CommissionModel`


- **max_leverage** (*float, optional*)－槓桿限制，預設 = **0.8**。  


- **limit_buy_multiplier** (*float, optional*)－
  - 買進／回補時的limit_price乘數，若有提供則limit_price = 下單時最近一筆收盤價 * `limit_buy_multiplier`。
  - 預設為**None**，代表**不設定**買進／回補時的limit_price。  


- **limit_sell_multiplier** (*float, optional*)－
  - 賣出／放空時的limit_price乘數，若有提供則limit_price = 下單時最近一筆收盤價 * `limit_sell_multiplier`。
  - 預設為**None**，代表**不設定**賣出／放空時的limit_price。  


- **allow_short** (*bool, optional*)－是否允許放空股票，預設為**False**，代表僅能做多。若設定為**True**，則pipeline中需要有`shorts`欄位。  


- **cancel_datedelta** (*int, optional*)－訂單幾天內未完全成交就取消。預設是在**下一次再平衡**時取消。 


- **custom_weight** (*bool, optional*)－
  - 是否要使用自訂的加權權數，預設為**False**，代表不使用（即等權重加權）。
  - 若設定為**True**，則pipeline中需要有`long_weights`（若`allow_short`=True，則也須有`short_weights`）欄位。


- **custom_loader** (*PipelineLoader , optional*)－
  - 用來取得價量以外資料，預設是**None**，代表不使用價量、`TQDataSet`與`TQAltDataSet`以外資料。
  - TQDataSet：`zipline.pipeline.data.TQDataSet`
  - TQAltDataSet：`zipline.pipeline.data.TQAltDataSet`
  - 目前支援的`PipelineLoader`：
    - DataFrameLoader（`zipline.pipeline.loaders.frame.DataFrameLoader`）。


- **pipeline** (*Pipeline*)－
  - 要用來產出交易清單或權重的pipeline，為<font color=DeepPink>**必要參數**</font>。
  - Pipeline：`zipline.pipeline.Pipeline`


- **analyze** (*callable[(context, pd.DataFrame) -> None], optional*)－
  - 傳入`analyze`函式以用於回測，函式中必須要有`context`與`perf`參數，預設是**None**。
  - 此函式在**回測結束**時被一次性呼叫，並繪製自訂圖表。


- **record_vars** (*callable[(context, BarData) -> None], optional*)－
  - 傳入`record_vars`函式以用於回測，函式中必須要有`context`與`data`參數，預設是**None**。
  - 此函式在**每個交易日結束**時被呼叫，並把指定資料紀錄於回測結果的DataFrame中。


- **get_record_vars** (*bool, optional*)－
  - 是否產出`record_vars`中`record`方法所記錄的變數，預設為**False**，代表不產出。
  - 若設定為**True**，則可用`algo.dict_record_vars`取出。


- **get_transaction_detail** (*bool, optional*)－
  - 是否產出交易結果，預設為**False**，代表不產出。
  - 若設定為**True**，則可用`algo.positions`、`algo.transactions`、`algo.orders`方式取出交易結果。


- **order_filling_policy** (*{'next_bar','current_bar'}, optional*)－
  - 設定交易方式，預設為**next_bar**，代表當天收盤後下單，隔一日收盤前成交，也就是原先的回測方式。
  - 若要當天開盤前下單，收盤前成交，則需指定設定為**current_bar**。

  
#### Returns:
    algo

#### Return type:
    zipline.algo.pipeline_algo.TargetPercentPipeAlgo


<br>




<br/>

**<font color=DeepPink>run</font>()**
>
> 執行演算法
> 
> **Returns:**
> - perf（回測報表）
> 
> **Return type:**
> - pd.DataFrame

<span id="date_rules"></span>
### **class zipline.api.<font color=DeepPink>date_rules</font>**
[Return to Menu](#menu)

- 為Factory API，主要用來傳入`zipline.api.schedule_function`的`date_rule`參數中。用來決定要以何種頻率觸發某項規則。
- 使用前請先import：`from zipline.api import date_rules`。
---

*static* **<font color=DeepPink>every_day</font>**()
> 
> 每日觸發某項規則。
> 
> **Returns:**
> - rule
> 
> **Return type:**
> - zipline.utils.events.EventRule

<br>

*static* **<font color=DeepPink>month_end</font>**(days_offset=0)
> 
> 每個月底觸發某項規則，並可以選擇性的新增一個偏移量。
> 
> **Parameters:**
> - **days_offset**(*int , optional*)：
>   - 在月底之前的第幾個交易日（由0開始計算）觸發某項規則。預設值是0，即在月底的最後一個交易日觸發。
>   - days_offset只能介於0~22之間。 
> 
> **Returns:**
> - rule
> 
> **Return type:**
> - zipline.utils.events.EventRule


<br>

*static* **<font color=DeepPink>month_start</font>**(days_offset=0)
> 
> 每個月初觸發某項規則，並可以選擇性的新增一個偏移量。
> 
> **Parameters:**
> - **days_offset**(*int , optional*)：
>   - 在月初之後的第幾個交易日（由0開始計算）才觸發某項規則。預設值是0，即在月初的第一個交易日觸發。
>   - days_offset只能介於0~22之間。 
> 
> **Returns:**
> - rule
> 
> **Return type:**
> - zipline.utils.events.EventRule


<br>

*static* **<font color=DeepPink>week_end</font>**(days_offset=0)
> 
> 在每周最後一個交易日觸發某項規則，並可以選擇性的新增一個偏移量。
> 
> **Parameters:**
> - **days_offset**(*int , optional*)：
>   - 在每周倒數第幾個交易日（由0開始計算）觸發某項規則。預設值是0，即在每周的最後一個交易日觸發。
>   - days_offset只能介於0~4之間。 
>
> **Returns:**
> - rule
> 
> **Return type:**
> - zipline.utils.events.EventRule


<br>

*static* **<font color=DeepPink>week_start</font>**(days_offset=0)
> 
> 在每周的第一個交易日觸發某項規則，並可以選擇性的新增一個偏移量。
> 
> **Parameters:**
> - **days_offset**(*int , optional*)：
>   - 在每周的第幾個交易日（由0開始計算）才觸發某項規則。預設值是0，即在每周的第一個交易日觸發。
>   - days_offset只能介於0~4之間。
> 
> **Returns:**
> - rule
> 
> **Return type:**
> - zipline.utils.events.EventRule

<span id="Examples"></span>
# 7. Examples
[Return to Menu](#menu)


```python
from zipline.utils.algo_instance import get_algo_instance, set_algo_instance
from zipline.algo.pipeline_algo import TargetPercentPipeAlgo
```

<span id="Case1"></span>
## Case 1 調整start_session與end_session

[Return to Menu](#menu)

僅調整`start_session`與`end_session`。其餘保持預設值。

以下設定pipeline（`make_pipeline()`），並定義`longs`欄位用來判斷須持有的股票。在`longs`欄位中要持有的股票標記為True，反之標記為False。


```python
from zipline.data import bundles

bundle = bundles.load(bundle_name)

def make_pipeline():
    rsi = RSI()
    longs = rsi.top(2, mask = ~SingleAsset(bundle.asset_finder.lookup_symbol('IR0001', as_of_date=None)))

    return Pipeline(
        
        columns = {
            "longs" : longs,
        }
    )
```


```python
algo_start = '2023-09-21'
algo_start_dt = pd.Timestamp(algo_start, tz = tz)

result = run_pipeline(make_pipeline(), algo_start, end)
result.query('longs == True')
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th></th>
      <th>longs</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="2" valign="top">2023-09-22 00:00:00+00:00</th>
      <th>Equity(6 [1590])</th>
      <td>True</td>
    </tr>
    <tr>
      <th>Equity(24 [2603])</th>
      <td>True</td>
    </tr>
    <tr>
      <th rowspan="2" valign="top">2023-09-25 00:00:00+00:00</th>
      <th>Equity(25 [2609])</th>
      <td>True</td>
    </tr>
    <tr>
      <th>Equity(34 [2886])</th>
      <td>True</td>
    </tr>
    <tr>
      <th rowspan="2" valign="top">2023-09-26 00:00:00+00:00</th>
      <th>Equity(33 [2885])</th>
      <td>True</td>
    </tr>
    <tr>
      <th>Equity(34 [2886])</th>
      <td>True</td>
    </tr>
    <tr>
      <th rowspan="2" valign="top">2023-09-27 00:00:00+00:00</th>
      <th>Equity(25 [2609])</th>
      <td>True</td>
    </tr>
    <tr>
      <th>Equity(34 [2886])</th>
      <td>True</td>
    </tr>
    <tr>
      <th rowspan="2" valign="top">2023-09-28 00:00:00+00:00</th>
      <th>Equity(6 [1590])</th>
      <td>True</td>
    </tr>
    <tr>
      <th>Equity(34 [2886])</th>
      <td>True</td>
    </tr>
    <tr>
      <th rowspan="2" valign="top">2023-10-02 00:00:00+00:00</th>
      <th>Equity(24 [2603])</th>
      <td>True</td>
    </tr>
    <tr>
      <th>Equity(34 [2886])</th>
      <td>True</td>
    </tr>
    <tr>
      <th rowspan="2" valign="top">2023-10-03 00:00:00+00:00</th>
      <th>Equity(6 [1590])</th>
      <td>True</td>
    </tr>
    <tr>
      <th>Equity(14 [2327])</th>
      <td>True</td>
    </tr>
  </tbody>
</table>
</div>



### 執行演算法
1. 實體化`TargetPercentPipeAlgo`並命名為`algo`。
2. 設定演算法：`set_algo_instance(algo)`
3. 執行演算法，並產出回測報表`stats`（*pd.DataFrame*）：`stats = algo.run()`


```python
algo = TargetPercentPipeAlgo(
                     start_session=algo_start_dt,
                     end_session=end_dt,
                     pipeline=make_pipeline,
)

# set_algo_instance
set_algo_instance(algo)

# run
stats = algo.run()
```

    [2024-03-13 02:41:05.491033]: INFO: rebalance: Cancel_order: current time: 2023-09-22 , created: 2023-09-21 , asset: Equity(46 [4904]), amount: 55478 , filled: 51900
    [2024-03-13 02:41:05.499675]: INFO: rebalance: Cancel_order: current time: 2023-09-25 , created: 2023-09-25 , asset: Equity(46 [4904]), amount: -14450 , filled: 0
    [2024-03-13 02:41:05.504262]: INFO: earn_dividends: Equity(6 [1590]), cash_dividend amount: 13.43905496, pay_date: 2023-10-30, div_owed: 54616.31935744
    [2024-03-13 02:41:05.505924]: INFO: handle_split: after split: asset: Equity(6 [1590]), amount: 4062, cost_basis: 982.73, last_sale_price: 981.0
    [2024-03-13 02:41:05.506228]: INFO: handle_split: returning cash: 643.94
    [2024-03-13 02:41:05.549641]: INFO: handle_simulation_end: Simulated 8 trading days
    first open: 2023-09-21 01:01:00+00:00
    last close: 2023-10-03 05:30:00+00:00
    

查看演算法中的參數設定


```python
algo
```




    TargetPercentPipeAlgo(
        sim_params = 
    SimulationParameters(
        start_session=2023-09-21 00:00:00+00:00,
        end_session=2023-10-03 00:00:00+00:00,
        capital_base=10000000.0,
        data_frequency=daily,
        emission_rate=daily,
        first_open=2023-09-21 01:01:00+00:00,
        last_close=2023-10-03 05:30:00+00:00,
        trading_calendar=<exchange_calendars.exchange_calendar_tejxtai.TEJ_XTAIExchangeCalendar object at 0x0000017B415030D0>
    ),
        benchmark = IR0001,
        zero treasury returns or not（if "True" then treasury returns = 0）= True,
        max_leverage = 0.8,
        slippage model used = VolumeShareSlippage(
        volume_limit=0.025,
        price_impact=0.1),
        commission_model = Custom_TW_Commission(min_trade_cost=20.0),
        liquidity_risk_management_rule = None,
        order_filling_policy = next_bar,
        adjust amount or not = False,
        limit_buy_multiplier = None,
        limit_sell_multiplier = None,
        allow short or not（if "False" then long only）= False,
        use custom weight or not（if not then "equal weighted"）= False,
        cancel_datedelta（if "None" then cancel open orders at next rebalance date）= None,
        stocklist = ['1101', '1216', '1301', '1303', '1326', '1402', '1590', '1605', '2002', '2207', '2301', '2303', '2308', '2317', '2327', '2330', '2345', '2357', '2379', '2382', '2395', '2408', '2412', '2454', '2603', '2609', '2615', '2801', '2880', '2881', '2882', '2883', '2884', '2885', '2886', '2887', '2890', '2891', '2892', '2912', '3008', '3034', '3037', '3045', '3231', '3711', '4904', '4938', '5871', '5876', '5880', '6415', '6505', '6669', '9910', 'IR0001'],
        tradeday = DatetimeIndex(['2023-09-21 00:00:00+00:00', '2023-09-22 00:00:00+00:00',
                   '2023-09-25 00:00:00+00:00', '2023-09-26 00:00:00+00:00',
                   '2023-09-27 00:00:00+00:00', '2023-09-28 00:00:00+00:00',
                   '2023-10-02 00:00:00+00:00', '2023-10-03 00:00:00+00:00'],
                  dtype='datetime64[ns, UTC]', freq='C'),
        rebalance_date_rule（If the "rebalance_date_rule" parameter is provided, then ignore the "tradeday" parameter."） 
        = None,
        get transaction detail or not = False,
        blotter = SimulationBlotter(
        slippage_models={<class 'zipline.assets._assets.Equity'>: VolumeShareSlippage(
        volume_limit=0.025,
        price_impact=0.1), <class 'zipline.assets._assets.Future'>: VolatilityVolumeShare(volume_limit=0.05, eta=<varies>)},
        commission_models={<class 'zipline.assets._assets.Equity'>: Custom_TW_Commission(min_trade_cost=20.0), <class 'zipline.assets._assets.Future'>: PerContract(cost_per_contract=0.85, exchange_fee=<varies>, min_trade_cost=0)},
        open_orders=defaultdict(<class 'list'>, {Equity(24 [2603]): [Order({'id': '307a0683715641db990a5ac44c1077bf', 'dt': Timestamp('2023-10-03 05:30:00+0000', tz='UTC'), 'reason': None, 'created': Timestamp('2023-10-03 05:30:00+0000', tz='UTC'), 'amount': -33982, 'filled': 0, 'commission': 0, 'stop': None, 'limit': None, 'stop_reached': False, 'limit_reached': False, 'sid': Equity(24 [2603]), 'status': <ORDER_STATUS.OPEN: 0>})], Equity(34 [2886]): [Order({'id': 'e5f6a3c19d2d4465ad5f0592eb6578c5', 'dt': Timestamp('2023-10-03 05:30:00+0000', tz='UTC'), 'reason': None, 'created': Timestamp('2023-10-03 05:30:00+0000', tz='UTC'), 'amount': -102579, 'filled': 0, 'commission': 0, 'stop': None, 'limit': None, 'stop_reached': False, 'limit_reached': False, 'sid': Equity(34 [2886]), 'status': <ORDER_STATUS.OPEN: 0>})], Equity(6 [1590]): [Order({'id': '4545f498f583429d8c867307ac34f9d5', 'dt': Timestamp('2023-10-03 05:30:00+0000', tz='UTC'), 'reason': None, 'created': Timestamp('2023-10-03 05:30:00+0000', tz='UTC'), 'amount': 3846, 'filled': 0, 'commission': 0, 'stop': None, 'limit': None, 'stop_reached': False, 'limit_reached': False, 'sid': Equity(6 [1590]), 'status': <ORDER_STATUS.OPEN: 0>})], Equity(14 [2327]): [Order({'id': '11c46077c2b549c9ab4c156f697053f6', 'dt': Timestamp('2023-10-03 05:30:00+0000', tz='UTC'), 'reason': None, 'created': Timestamp('2023-10-03 05:30:00+0000', tz='UTC'), 'amount': 7381, 'filled': 0, 'commission': 0, 'stop': None, 'limit': None, 'stop_reached': False, 'limit_reached': False, 'sid': Equity(14 [2327]), 'status': <ORDER_STATUS.OPEN: 0>})]}),
        orders={'bdcb9758340444948f3af991c6b2478b': Order({'id': 'bdcb9758340444948f3af991c6b2478b', 'dt': Timestamp('2023-09-22 05:30:00+0000', tz='UTC'), 'reason': None, 'created': Timestamp('2023-09-21 05:30:00+0000', tz='UTC'), 'amount': 106951, 'filled': 106951, 'commission': 5761, 'stop': None, 'limit': None, 'stop_reached': False, 'limit_reached': False, 'sid': Equity(34 [2886]), 'status': <ORDER_STATUS.FILLED: 1>}), 'acc3c2d89f8e48c3bc42daa9e8d81560': Order({'id': 'acc3c2d89f8e48c3bc42daa9e8d81560', 'dt': Timestamp('2023-09-22 05:30:00+0000', tz='UTC'), 'reason': None, 'created': Timestamp('2023-09-21 05:30:00+0000', tz='UTC'), 'amount': 55478, 'filled': 51900, 'commission': 5333, 'stop': None, 'limit': None, 'stop_reached': False, 'limit_reached': False, 'sid': Equity(46 [4904]), 'status': <ORDER_STATUS.CANCELLED: 2>}), '91e9060255b8480c88251ce5ed781b49': Order({'id': '91e9060255b8480c88251ce5ed781b49', 'dt': Timestamp('2023-09-25 05:30:00+0000', tz='UTC'), 'reason': None, 'created': Timestamp('2023-09-22 05:30:00+0000', tz='UTC'), 'amount': -106951, 'filled': -106951, 'commission': 17985, 'stop': None, 'limit': None, 'stop_reached': False, 'limit_reached': False, 'sid': Equity(34 [2886]), 'status': <ORDER_STATUS.FILLED: 1>}), '71a11f3296354834956e3a801721c806': Order({'id': '71a11f3296354834956e3a801721c806', 'dt': Timestamp('2023-09-25 05:30:00+0000', tz='UTC'), 'reason': None, 'created': Timestamp('2023-09-22 05:30:00+0000', tz='UTC'), 'amount': -51900, 'filled': -37450, 'commission': 12081, 'stop': None, 'limit': None, 'stop_reached': False, 'limit_reached': False, 'sid': Equity(46 [4904]), 'status': <ORDER_STATUS.CANCELLED: 2>}), '2777dbff1bbd48188466187cc31e69d3': Order({'id': '2777dbff1bbd48188466187cc31e69d3', 'dt': Timestamp('2023-09-25 05:30:00+0000', tz='UTC'), 'reason': None, 'created': Timestamp('2023-09-22 05:30:00+0000', tz='UTC'), 'amount': 34894, 'filled': 34894, 'commission': 5768, 'stop': None, 'limit': None, 'stop_reached': False, 'limit_reached': False, 'sid': Equity(24 [2603]), 'status': <ORDER_STATUS.FILLED: 1>}), '1ec8dcf092f84220a87650c5db8959fc': Order({'id': '1ec8dcf092f84220a87650c5db8959fc', 'dt': Timestamp('2023-09-25 05:30:00+0000', tz='UTC'), 'reason': None, 'created': Timestamp('2023-09-22 05:30:00+0000', tz='UTC'), 'amount': 4064, 'filled': 4064, 'commission': 5682, 'stop': None, 'limit': None, 'stop_reached': False, 'limit_reached': False, 'sid': Equity(6 [1590]), 'status': <ORDER_STATUS.FILLED: 1>}), 'f39524a1508142c9af7306307956f8e9': Order({'id': 'f39524a1508142c9af7306307956f8e9', 'dt': Timestamp('2023-09-25 05:30:00+0000', tz='UTC'), 'reason': None, 'created': Timestamp('2023-09-25 05:30:00+0000', tz='UTC'), 'amount': -14450, 'filled': 0, 'commission': 0, 'stop': None, 'limit': None, 'stop_reached': False, 'limit_reached': False, 'sid': Equity(46 [4904]), 'status': <ORDER_STATUS.CANCELLED: 2>}), 'dc7128121a9d40d182ec3d9d4d465492': Order({'id': 'dc7128121a9d40d182ec3d9d4d465492', 'dt': Timestamp('2023-09-26 05:30:00+0000', tz='UTC'), 'reason': None, 'created': Timestamp('2023-09-25 05:30:00+0000', tz='UTC'), 'amount': -34894, 'filled': -34894, 'commission': 17835, 'stop': None, 'limit': None, 'stop_reached': False, 'limit_reached': False, 'sid': Equity(24 [2603]), 'status': <ORDER_STATUS.FILLED: 1>}), 'ceec5c24911044baba3d8c0abab96960': Order({'id': 'ceec5c24911044baba3d8c0abab96960', 'dt': Timestamp('2023-09-26 05:30:00+0000', tz='UTC'), 'reason': None, 'created': Timestamp('2023-09-25 05:30:00+0000', tz='UTC'), 'amount': -14450, 'filled': -14450, 'commission': 4649, 'stop': None, 'limit': None, 'stop_reached': False, 'limit_reached': False, 'sid': Equity(46 [4904]), 'status': <ORDER_STATUS.FILLED: 1>}), 'c55dc4e5611246698718518b5ad46860': Order({'id': 'c55dc4e5611246698718518b5ad46860', 'dt': Timestamp('2023-09-26 05:30:00+0000', tz='UTC'), 'reason': None, 'created': Timestamp('2023-09-25 05:30:00+0000', tz='UTC'), 'amount': -4062, 'filled': -4062, 'commission': 17221, 'stop': None, 'limit': None, 'stop_reached': False, 'limit_reached': False, 'sid': Equity(6 [1590]), 'status': <ORDER_STATUS.FILLED: 1>}), '5ca0608910f8443a9c2ec23de6c1ab6f': Order({'id': '5ca0608910f8443a9c2ec23de6c1ab6f', 'dt': Timestamp('2023-09-26 05:30:00+0000', tz='UTC'), 'reason': None, 'created': Timestamp('2023-09-25 05:30:00+0000', tz='UTC'), 'amount': 87136, 'filled': 87136, 'commission': 5706, 'stop': None, 'limit': None, 'stop_reached': False, 'limit_reached': False, 'sid': Equity(25 [2609]), 'status': <ORDER_STATUS.FILLED: 1>}), 'aafbcc931d8e4f87adb21686aeb0f227': Order({'id': 'aafbcc931d8e4f87adb21686aeb0f227', 'dt': Timestamp('2023-09-26 05:30:00+0000', tz='UTC'), 'reason': None, 'created': Timestamp('2023-09-25 05:30:00+0000', tz='UTC'), 'amount': 105366, 'filled': 105366, 'commission': 5684, 'stop': None, 'limit': None, 'stop_reached': False, 'limit_reached': False, 'sid': Equity(34 [2886]), 'status': <ORDER_STATUS.FILLED: 1>}), '153b35815aa44224aebbd79d673403b2': Order({'id': '153b35815aa44224aebbd79d673403b2', 'dt': Timestamp('2023-09-27 05:30:00+0000', tz='UTC'), 'reason': None, 'created': Timestamp('2023-09-26 05:30:00+0000', tz='UTC'), 'amount': -87136, 'filled': -87136, 'commission': 17699, 'stop': None, 'limit': None, 'stop_reached': False, 'limit_reached': False, 'sid': Equity(25 [2609]), 'status': <ORDER_STATUS.FILLED: 1>}), 'b397a24cd315420683b711b7c933abc3': Order({'id': 'b397a24cd315420683b711b7c933abc3', 'dt': Timestamp('2023-09-27 05:30:00+0000', tz='UTC'), 'reason': None, 'created': Timestamp('2023-09-26 05:30:00+0000', tz='UTC'), 'amount': 157495, 'filled': 157495, 'commission': 5645, 'stop': None, 'limit': None, 'stop_reached': False, 'limit_reached': False, 'sid': Equity(33 [2885]), 'status': <ORDER_STATUS.FILLED: 1>}), '2f3a95e39fd840dc8aca520f0c36f6b1': Order({'id': '2f3a95e39fd840dc8aca520f0c36f6b1', 'dt': Timestamp('2023-09-27 05:30:00+0000', tz='UTC'), 'reason': None, 'created': Timestamp('2023-09-26 05:30:00+0000', tz='UTC'), 'amount': -1339, 'filled': -1339, 'commission': 224, 'stop': None, 'limit': None, 'stop_reached': False, 'limit_reached': False, 'sid': Equity(34 [2886]), 'status': <ORDER_STATUS.FILLED: 1>}), '1ebe571a863a415297cd8aa1bc2b07b9': Order({'id': '1ebe571a863a415297cd8aa1bc2b07b9', 'dt': Timestamp('2023-09-28 05:30:00+0000', tz='UTC'), 'reason': None, 'created': Timestamp('2023-09-27 05:30:00+0000', tz='UTC'), 'amount': -157495, 'filled': -157495, 'commission': 17458, 'stop': None, 'limit': None, 'stop_reached': False, 'limit_reached': False, 'sid': Equity(33 [2885]), 'status': <ORDER_STATUS.FILLED: 1>}), '432d560b9e99476d846b3675db5b2d44': Order({'id': '432d560b9e99476d846b3675db5b2d44', 'dt': Timestamp('2023-09-28 05:30:00+0000', tz='UTC'), 'reason': None, 'created': Timestamp('2023-09-27 05:30:00+0000', tz='UTC'), 'amount': 85400, 'filled': 85400, 'commission': 5532, 'stop': None, 'limit': None, 'stop_reached': False, 'limit_reached': False, 'sid': Equity(25 [2609]), 'status': <ORDER_STATUS.FILLED: 1>}), '9fb1d769ef504f44b2dd544653e997c2': Order({'id': '9fb1d769ef504f44b2dd544653e997c2', 'dt': Timestamp('2023-09-28 05:30:00+0000', tz='UTC'), 'reason': None, 'created': Timestamp('2023-09-27 05:30:00+0000', tz='UTC'), 'amount': -51, 'filled': -51, 'commission': 26.0, 'stop': None, 'limit': None, 'stop_reached': False, 'limit_reached': False, 'sid': Equity(34 [2886]), 'status': <ORDER_STATUS.FILLED: 1>}), '5a76b96aecf749d683699d19b7fc687c': Order({'id': '5a76b96aecf749d683699d19b7fc687c', 'dt': Timestamp('2023-10-02 05:30:00+0000', tz='UTC'), 'reason': None, 'created': Timestamp('2023-09-28 05:30:00+0000', tz='UTC'), 'amount': -85400, 'filled': -85400, 'commission': 16799, 'stop': None, 'limit': None, 'stop_reached': False, 'limit_reached': False, 'sid': Equity(25 [2609]), 'status': <ORDER_STATUS.FILLED: 1>}), 'c2f1f413dea8434f9ef69b9f58c40810': Order({'id': 'c2f1f413dea8434f9ef69b9f58c40810', 'dt': Timestamp('2023-10-02 05:30:00+0000', tz='UTC'), 'reason': None, 'created': Timestamp('2023-09-28 05:30:00+0000', tz='UTC'), 'amount': -412, 'filled': -412, 'commission': 70, 'stop': None, 'limit': None, 'stop_reached': False, 'limit_reached': False, 'sid': Equity(34 [2886]), 'status': <ORDER_STATUS.FILLED: 1>}), 'c2c40f199a7f4f7a85e33a6f0fd9c3d7': Order({'id': 'c2c40f199a7f4f7a85e33a6f0fd9c3d7', 'dt': Timestamp('2023-10-02 05:30:00+0000', tz='UTC'), 'reason': None, 'created': Timestamp('2023-09-28 05:30:00+0000', tz='UTC'), 'amount': 3988, 'filled': 3988, 'commission': 5712, 'stop': None, 'limit': None, 'stop_reached': False, 'limit_reached': False, 'sid': Equity(6 [1590]), 'status': <ORDER_STATUS.FILLED: 1>}), '40c0549e04914d8db877272facf233f9': Order({'id': '40c0549e04914d8db877272facf233f9', 'dt': Timestamp('2023-10-03 05:30:00+0000', tz='UTC'), 'reason': None, 'created': Timestamp('2023-10-02 05:30:00+0000', tz='UTC'), 'amount': -3988, 'filled': -3988, 'commission': 17612, 'stop': None, 'limit': None, 'stop_reached': False, 'limit_reached': False, 'sid': Equity(6 [1590]), 'status': <ORDER_STATUS.FILLED: 1>}), 'b990b03de87744508a9bfcbab7e14ba6': Order({'id': 'b990b03de87744508a9bfcbab7e14ba6', 'dt': Timestamp('2023-10-03 05:30:00+0000', tz='UTC'), 'reason': None, 'created': Timestamp('2023-10-02 05:30:00+0000', tz='UTC'), 'amount': 33982, 'filled': 33982, 'commission': 5351, 'stop': None, 'limit': None, 'stop_reached': False, 'limit_reached': False, 'sid': Equity(24 [2603]), 'status': <ORDER_STATUS.FILLED: 1>}), 'fcc61389fce5448297c04514a9523155': Order({'id': 'fcc61389fce5448297c04514a9523155', 'dt': Timestamp('2023-10-03 05:30:00+0000', tz='UTC'), 'reason': None, 'created': Timestamp('2023-10-02 05:30:00+0000', tz='UTC'), 'amount': -985, 'filled': -985, 'commission': 165, 'stop': None, 'limit': None, 'stop_reached': False, 'limit_reached': False, 'sid': Equity(34 [2886]), 'status': <ORDER_STATUS.FILLED: 1>}), '307a0683715641db990a5ac44c1077bf': Order({'id': '307a0683715641db990a5ac44c1077bf', 'dt': Timestamp('2023-10-03 05:30:00+0000', tz='UTC'), 'reason': None, 'created': Timestamp('2023-10-03 05:30:00+0000', tz='UTC'), 'amount': -33982, 'filled': 0, 'commission': 0, 'stop': None, 'limit': None, 'stop_reached': False, 'limit_reached': False, 'sid': Equity(24 [2603]), 'status': <ORDER_STATUS.OPEN: 0>}), 'e5f6a3c19d2d4465ad5f0592eb6578c5': Order({'id': 'e5f6a3c19d2d4465ad5f0592eb6578c5', 'dt': Timestamp('2023-10-03 05:30:00+0000', tz='UTC'), 'reason': None, 'created': Timestamp('2023-10-03 05:30:00+0000', tz='UTC'), 'amount': -102579, 'filled': 0, 'commission': 0, 'stop': None, 'limit': None, 'stop_reached': False, 'limit_reached': False, 'sid': Equity(34 [2886]), 'status': <ORDER_STATUS.OPEN: 0>}), '4545f498f583429d8c867307ac34f9d5': Order({'id': '4545f498f583429d8c867307ac34f9d5', 'dt': Timestamp('2023-10-03 05:30:00+0000', tz='UTC'), 'reason': None, 'created': Timestamp('2023-10-03 05:30:00+0000', tz='UTC'), 'amount': 3846, 'filled': 0, 'commission': 0, 'stop': None, 'limit': None, 'stop_reached': False, 'limit_reached': False, 'sid': Equity(6 [1590]), 'status': <ORDER_STATUS.OPEN: 0>}), '11c46077c2b549c9ab4c156f697053f6': Order({'id': '11c46077c2b549c9ab4c156f697053f6', 'dt': Timestamp('2023-10-03 05:30:00+0000', tz='UTC'), 'reason': None, 'created': Timestamp('2023-10-03 05:30:00+0000', tz='UTC'), 'amount': 7381, 'filled': 0, 'commission': 0, 'stop': None, 'limit': None, 'stop_reached': False, 'limit_reached': False, 'sid': Equity(14 [2327]), 'status': <ORDER_STATUS.OPEN: 0>})},
        new_orders=[],
        current_dt=2023-10-03 05:30:00+00:00),
        recorded_vars = {})




```python
stats.T
```




<div style="overflow-x: auto; white-space: nowrap;">
    <table border="1" class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>2023-09-21 13:30:00+08:00</th>
          <th>2023-09-22 13:30:00+08:00</th>
          <th>2023-09-25 13:30:00+08:00</th>
          <th>2023-09-26 13:30:00+08:00</th>
          <th>2023-09-27 13:30:00+08:00</th>
          <th>2023-09-28 13:30:00+08:00</th>
          <th>2023-10-02 13:30:00+08:00</th>
          <th>2023-10-03 13:30:00+08:00</th>
        </tr>
      </thead>
      <tbody>
        <tr><th>period_open</th><td>2023-09-21 09:01:00+08:00</td><td>2023-09-22 09:01:00+08:00</td><td>2023-09-25 09:01:00+08:00</td><td>2023-09-26 09:01:00+08:00</td><td>2023-09-27 09:01:00+08:00</td><td>2023-09-28 09:01:00+08:00</td><td>2023-10-02 09:01:00+08:00</td><td>2023-10-03 09:01:00+08:00</td></tr>
        <tr><th>period_close</th><td>2023-09-21 13:30:00+08:00</td><td>2023-09-22 13:30:00+08:00</td><td>2023-09-25 13:30:00+08:00</td><td>2023-09-26 13:30:00+08:00</td><td>2023-09-27 13:30:00+08:00</td><td>2023-09-28 13:30:00+08:00</td><td>2023-10-02 13:30:00+08:00</td><td>2023-10-03 13:30:00+08:00</td></tr>
        <tr><th>starting_cash</th><td>10000000.0</td><td>10000000.0</td><td>2203905.142427</td><td>921903.876036</td><td>1851480.446535</td><td>1916878.693942</td><td>1959492.659321</td><td>1740449.09152</td></tr>
        <tr><th>ending_cash</th><td>10000000.0</td><td>2203905.142427</td><td>921903.876036</td><td>1851480.446535</td><td>1916878.693942</td><td>1959492.659321</td><td>1740449.09152</td><td>1979378.550691</td></tr>
        <tr><th>portfolio_value</th><td>10000000.0</td><td>9988642.942427</td><td>10009796.876036</td><td>9843482.746535</td><td>9799695.843942</td><td>9760817.859321</td><td>9642395.49152</td><td>9596488.900691</td></tr>
        <tr><th>transactions</th><td>[]</td><td>[{'amount': 106951, 'dt': '2023-09-22 13:30:00+08:00'}]</td><td>[{'amount': -37450, 'dt': '2023-09-25 13:30:00+08:00'}]</td><td>[{'amount': -14450, 'dt': '2023-09-26 13:30:00+08:00'}]</td><td>[{'amount': -87136, 'dt': '2023-09-27 13:30:00+08:00'}]</td><td>[{'amount': -157495, 'dt': '2023-09-28 13:30:00+08:00'}]</td><td>[{'amount': -85400, 'dt': '2023-10-02 13:30:00+08:00'}]</td><td>[{'amount': -3988, 'dt': '2023-10-03 13:30:00+08:00'}]</td></tr>
        <tr><th>longs_count</th><td>0</td><td>2</td><td>3</td><td>2</td><td>2</td><td>2</td><td>2</td><td>2</td></tr>
        <tr><th>gross_leverage</th><td>0.0</td><td>0.779359</td><td>0.9079</td><td>0.811908</td><td>0.804394</td><td>0.799249</td><td>0.8195</td><td>0.793739</td></tr>
        <tr><th>sharpe</th><td>NaN</td><td>-11.224972</td><td>3.147118</td><td>-7.232764</td><td>-8.577955</td><td>-9.570898</td><td>-12.063911</td><td>-12.898619</td></tr>
        <tr><th>trading_days</th><td>1</td><td>2</td><td>3</td><td>4</td><td>5</td><td>6</td><td>7</td><td>8</td></tr>
        <tr><th>returns</th><td>0.0</td><td>-0.001136</td><td>0.002118</td><td>-0.016615</td><td>-0.004448</td><td>-0.003967</td><td>-0.012132</td><td>-0.004761</td></tr>
        <tr><th>alpha</th><td>NaN</td><td>-0.223982</td><td>0.114994</td><td>-0.409947</td><td>-0.5193</td><td>-0.56714</td><td>-0.729949</td><td>-0.723783</td></tr>
        <tr><th>beta</th><td>NaN</td><td>-0.076255</td><td>0.064179</td><td>0.468458</td><td>0.415352</td><td>0.385683</td><td>0.052787</td><td>0.044894</td></tr>
      </tbody>
    </table>
</div>





```python
positions, transactions, orders = get_transaction_detail(stats)
```


```python
transactions
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>sid</th>
      <th>symbol</th>
      <th>amount</th>
      <th>dt</th>
      <th>price</th>
      <th>order_id</th>
      <th>asset</th>
      <th>commission</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2023-09-22 13:30:00+08:00</th>
      <td>34</td>
      <td>2886</td>
      <td>106951</td>
      <td>2023-09-22 13:30:00+08:00</td>
      <td>37.800273</td>
      <td>bdcb9758340444948f3af991c6b2478b</td>
      <td>Equity(34 [2886])</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2023-09-22 13:30:00+08:00</th>
      <td>46</td>
      <td>4904</td>
      <td>51900</td>
      <td>2023-09-22 13:30:00+08:00</td>
      <td>72.104506</td>
      <td>acc3c2d89f8e48c3bc42daa9e8d81560</td>
      <td>Equity(46 [4904])</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2023-09-25 13:30:00+08:00</th>
      <td>46</td>
      <td>4904</td>
      <td>-37450</td>
      <td>2023-09-25 13:30:00+08:00</td>
      <td>72.895444</td>
      <td>71a11f3296354834956e3a801721c806</td>
      <td>Equity(46 [4904])</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2023-09-25 13:30:00+08:00</th>
      <td>34</td>
      <td>2886</td>
      <td>-106951</td>
      <td>2023-09-25 13:30:00+08:00</td>
      <td>37.999742</td>
      <td>91e9060255b8480c88251ce5ed781b49</td>
      <td>Equity(34 [2886])</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2023-09-25 13:30:00+08:00</th>
      <td>24</td>
      <td>2603</td>
      <td>34894</td>
      <td>2023-09-25 13:30:00+08:00</td>
      <td>116.000161</td>
      <td>2777dbff1bbd48188466187cc31e69d3</td>
      <td>Equity(24 [2603])</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2023-09-25 13:30:00+08:00</th>
      <td>6</td>
      <td>1590</td>
      <td>4064</td>
      <td>2023-09-25 13:30:00+08:00</td>
      <td>981.008970</td>
      <td>1ec8dcf092f84220a87650c5db8959fc</td>
      <td>Equity(6 [1590])</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2023-09-26 13:30:00+08:00</th>
      <td>46</td>
      <td>4904</td>
      <td>-14450</td>
      <td>2023-09-26 13:30:00+08:00</td>
      <td>72.699787</td>
      <td>ceec5c24911044baba3d8c0abab96960</td>
      <td>Equity(46 [4904])</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2023-09-26 13:30:00+08:00</th>
      <td>24</td>
      <td>2603</td>
      <td>-34894</td>
      <td>2023-09-26 13:30:00+08:00</td>
      <td>115.499884</td>
      <td>dc7128121a9d40d182ec3d9d4d465492</td>
      <td>Equity(24 [2603])</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2023-09-26 13:30:00+08:00</th>
      <td>6</td>
      <td>1590</td>
      <td>-4062</td>
      <td>2023-09-26 13:30:00+08:00</td>
      <td>957.984660</td>
      <td>c55dc4e5611246698718518b5ad46860</td>
      <td>Equity(6 [1590])</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2023-09-26 13:30:00+08:00</th>
      <td>25</td>
      <td>2609</td>
      <td>87136</td>
      <td>2023-09-26 13:30:00+08:00</td>
      <td>45.950388</td>
      <td>5ca0608910f8443a9c2ec23de6c1ab6f</td>
      <td>Equity(25 [2609])</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2023-09-26 13:30:00+08:00</th>
      <td>34</td>
      <td>2886</td>
      <td>105366</td>
      <td>2023-09-26 13:30:00+08:00</td>
      <td>37.850331</td>
      <td>aafbcc931d8e4f87adb21686aeb0f227</td>
      <td>Equity(34 [2886])</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2023-09-27 13:30:00+08:00</th>
      <td>25</td>
      <td>2609</td>
      <td>-87136</td>
      <td>2023-09-27 13:30:00+08:00</td>
      <td>45.899835</td>
      <td>153b35815aa44224aebbd79d673403b2</td>
      <td>Equity(25 [2609])</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2023-09-27 13:30:00+08:00</th>
      <td>33</td>
      <td>2885</td>
      <td>157495</td>
      <td>2023-09-27 13:30:00+08:00</td>
      <td>25.150272</td>
      <td>b397a24cd315420683b711b7c933abc3</td>
      <td>Equity(33 [2885])</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2023-09-27 13:30:00+08:00</th>
      <td>34</td>
      <td>2886</td>
      <td>-1339</td>
      <td>2023-09-27 13:30:00+08:00</td>
      <td>37.700000</td>
      <td>2f3a95e39fd840dc8aca520f0c36f6b1</td>
      <td>Equity(34 [2886])</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2023-09-28 13:30:00+08:00</th>
      <td>33</td>
      <td>2885</td>
      <td>-157495</td>
      <td>2023-09-28 13:30:00+08:00</td>
      <td>25.049461</td>
      <td>1ebe571a863a415297cd8aa1bc2b07b9</td>
      <td>Equity(33 [2885])</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2023-09-28 13:30:00+08:00</th>
      <td>25</td>
      <td>2609</td>
      <td>85400</td>
      <td>2023-09-28 13:30:00+08:00</td>
      <td>45.450323</td>
      <td>432d560b9e99476d846b3675db5b2d44</td>
      <td>Equity(25 [2609])</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2023-09-28 13:30:00+08:00</th>
      <td>34</td>
      <td>2886</td>
      <td>-51</td>
      <td>2023-09-28 13:30:00+08:00</td>
      <td>37.700000</td>
      <td>9fb1d769ef504f44b2dd544653e997c2</td>
      <td>Equity(34 [2886])</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2023-10-02 13:30:00+08:00</th>
      <td>25</td>
      <td>2609</td>
      <td>-85400</td>
      <td>2023-10-02 13:30:00+08:00</td>
      <td>44.449880</td>
      <td>5a76b96aecf749d683699d19b7fc687c</td>
      <td>Equity(25 [2609])</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2023-10-02 13:30:00+08:00</th>
      <td>34</td>
      <td>2886</td>
      <td>-412</td>
      <td>2023-10-02 13:30:00+08:00</td>
      <td>37.600000</td>
      <td>c2f1f413dea8434f9ef69b9f58c40810</td>
      <td>Equity(34 [2886])</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2023-10-02 13:30:00+08:00</th>
      <td>6</td>
      <td>1590</td>
      <td>3988</td>
      <td>2023-10-02 13:30:00+08:00</td>
      <td>1005.008408</td>
      <td>c2c40f199a7f4f7a85e33a6f0fd9c3d7</td>
      <td>Equity(6 [1590])</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2023-10-03 13:30:00+08:00</th>
      <td>6</td>
      <td>1590</td>
      <td>-3988</td>
      <td>2023-10-03 13:30:00+08:00</td>
      <td>997.990178</td>
      <td>40c0549e04914d8db877272facf233f9</td>
      <td>Equity(6 [1590])</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2023-10-03 13:30:00+08:00</th>
      <td>24</td>
      <td>2603</td>
      <td>33982</td>
      <td>2023-10-03 13:30:00+08:00</td>
      <td>110.500048</td>
      <td>b990b03de87744508a9bfcbab7e14ba6</td>
      <td>Equity(24 [2603])</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2023-10-03 13:30:00+08:00</th>
      <td>34</td>
      <td>2886</td>
      <td>-985</td>
      <td>2023-10-03 13:30:00+08:00</td>
      <td>37.650000</td>
      <td>fcc61389fce5448297c04514a9523155</td>
      <td>Equity(34 [2886])</td>
      <td>None</td>
    </tr>
  </tbody>
</table>
</div>




```python
positions['mv'] = positions['amount'] * positions['last_sale_price']
positions
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>sid</th>
      <th>symbol</th>
      <th>asset</th>
      <th>amount</th>
      <th>cost_basis</th>
      <th>last_sale_price</th>
      <th>mv</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2023-09-22 13:30:00+08:00</th>
      <td>34</td>
      <td>2886</td>
      <td>Equity(34 [2886])</td>
      <td>106951</td>
      <td>37.854139</td>
      <td>37.80</td>
      <td>4042747.80</td>
    </tr>
    <tr>
      <th>2023-09-22 13:30:00+08:00</th>
      <td>46</td>
      <td>4904</td>
      <td>Equity(46 [4904])</td>
      <td>51900</td>
      <td>72.207262</td>
      <td>72.10</td>
      <td>3741990.00</td>
    </tr>
    <tr>
      <th>2023-09-25 13:30:00+08:00</th>
      <td>46</td>
      <td>4904</td>
      <td>Equity(46 [4904])</td>
      <td>14450</td>
      <td>73.043317</td>
      <td>72.90</td>
      <td>1053405.00</td>
    </tr>
    <tr>
      <th>2023-09-25 13:30:00+08:00</th>
      <td>24</td>
      <td>2603</td>
      <td>Equity(24 [2603])</td>
      <td>34894</td>
      <td>116.165462</td>
      <td>116.00</td>
      <td>4047704.00</td>
    </tr>
    <tr>
      <th>2023-09-25 13:30:00+08:00</th>
      <td>6</td>
      <td>1590</td>
      <td>Equity(6 [1590])</td>
      <td>4064</td>
      <td>982.407100</td>
      <td>981.00</td>
      <td>3986784.00</td>
    </tr>
    <tr>
      <th>2023-09-26 13:30:00+08:00</th>
      <td>25</td>
      <td>2609</td>
      <td>Equity(25 [2609])</td>
      <td>87136</td>
      <td>46.015872</td>
      <td>45.95</td>
      <td>4003899.20</td>
    </tr>
    <tr>
      <th>2023-09-26 13:30:00+08:00</th>
      <td>34</td>
      <td>2886</td>
      <td>Equity(34 [2886])</td>
      <td>105366</td>
      <td>37.904276</td>
      <td>37.85</td>
      <td>3988103.10</td>
    </tr>
    <tr>
      <th>2023-09-27 13:30:00+08:00</th>
      <td>34</td>
      <td>2886</td>
      <td>Equity(34 [2886])</td>
      <td>104027</td>
      <td>37.906429</td>
      <td>37.70</td>
      <td>3921817.90</td>
    </tr>
    <tr>
      <th>2023-09-27 13:30:00+08:00</th>
      <td>33</td>
      <td>2885</td>
      <td>Equity(33 [2885])</td>
      <td>157495</td>
      <td>25.186114</td>
      <td>25.15</td>
      <td>3960999.25</td>
    </tr>
    <tr>
      <th>2023-09-28 13:30:00+08:00</th>
      <td>34</td>
      <td>2886</td>
      <td>Equity(34 [2886])</td>
      <td>103976</td>
      <td>37.906679</td>
      <td>37.70</td>
      <td>3919895.20</td>
    </tr>
    <tr>
      <th>2023-09-28 13:30:00+08:00</th>
      <td>25</td>
      <td>2609</td>
      <td>Equity(25 [2609])</td>
      <td>85400</td>
      <td>45.515101</td>
      <td>45.45</td>
      <td>3881430.00</td>
    </tr>
    <tr>
      <th>2023-10-02 13:30:00+08:00</th>
      <td>34</td>
      <td>2886</td>
      <td>Equity(34 [2886])</td>
      <td>103564</td>
      <td>37.907355</td>
      <td>37.60</td>
      <td>3894006.40</td>
    </tr>
    <tr>
      <th>2023-10-02 13:30:00+08:00</th>
      <td>6</td>
      <td>1590</td>
      <td>Equity(6 [1590])</td>
      <td>3988</td>
      <td>1006.440705</td>
      <td>1005.00</td>
      <td>4007940.00</td>
    </tr>
    <tr>
      <th>2023-10-03 13:30:00+08:00</th>
      <td>34</td>
      <td>2886</td>
      <td>Equity(34 [2886])</td>
      <td>102579</td>
      <td>37.908964</td>
      <td>37.65</td>
      <td>3862099.35</td>
    </tr>
    <tr>
      <th>2023-10-03 13:30:00+08:00</th>
      <td>24</td>
      <td>2603</td>
      <td>Equity(24 [2603])</td>
      <td>33982</td>
      <td>110.657513</td>
      <td>110.50</td>
      <td>3755011.00</td>
    </tr>
  </tbody>
</table>
</div>




```python
stats.net_leverage
```




    2023-09-21 13:30:00+08:00    0.000000
    2023-09-22 13:30:00+08:00    0.779359
    2023-09-25 13:30:00+08:00    0.907900
    2023-09-26 13:30:00+08:00    0.811908
    2023-09-27 13:30:00+08:00    0.804394
    2023-09-28 13:30:00+08:00    0.799249
    2023-10-02 13:30:00+08:00    0.819500
    2023-10-03 13:30:00+08:00    0.793739
    Name: net_leverage, dtype: float64



<span id="Case2"></span>
## Case 2 調整max_leverage
[Return to Menu](#menu)

接續**Case 1**，多調整`max_leverage=0.70`，其餘與**Case 1**相同。


```python
algo = TargetPercentPipeAlgo(
                     start_session=algo_start_dt,
                     end_session=end_dt,
                     max_leverage=0.70,
                     pipeline=make_pipeline,
)

# set_algo_instance
set_algo_instance(algo)

# run
stats = algo.run()
```

    [2024-03-13 02:41:05.749665]: INFO: rebalance: Cancel_order: current time: 2023-09-25 , created: 2023-09-25 , asset: Equity(46 [4904]), amount: -11093 , filled: 0
    [2024-03-13 02:41:05.753066]: INFO: earn_dividends: Equity(6 [1590]), cash_dividend amount: 13.43905496, pay_date: 2023-10-30, div_owed: 47789.27943776
    [2024-03-13 02:41:05.754062]: INFO: handle_split: after split: asset: Equity(6 [1590]), amount: 3554, cost_basis: 982.73, last_sale_price: 981.0
    [2024-03-13 02:41:05.754062]: INFO: handle_split: returning cash: 809.13
    [2024-03-13 02:41:05.795164]: INFO: handle_simulation_end: Simulated 8 trading days
    first open: 2023-09-21 01:01:00+00:00
    last close: 2023-10-03 05:30:00+00:00
    


```python
stats.net_leverage
```




    2023-09-21 13:30:00+08:00    0.000000
    2023-09-22 13:30:00+08:00    0.704456
    2023-09-25 13:30:00+08:00    0.783204
    2023-09-26 13:30:00+08:00    0.708862
    2023-09-27 13:30:00+08:00    0.703443
    2023-09-28 13:30:00+08:00    0.698989
    2023-10-02 13:30:00+08:00    0.715883
    2023-10-03 13:30:00+08:00    0.694088
    Name: net_leverage, dtype: float64



<span id="Case3"></span>
## Case 3 調整tradeday
[Return to Menu](#menu)  

接續**Case 1**，多新增`tradeday`，其餘與**Case 1**相同。


```python
# 設定再平衡日期
freq = 'MS'   # QS-JUL  MS W
_tradeday = list(pd.date_range(start=start_dt, end=end_dt, freq=freq))
tradeday = [get_calendar(calendar_name).next_open(pd.Timestamp(i)).strftime('%Y-%m-%d') if \
           get_calendar(calendar_name).is_session(i)==False else i.strftime('%Y-%m-%d') for i in _tradeday]
tradeday
```




    ['2023-06-01', '2023-07-03', '2023-08-01', '2023-09-01', '2023-10-02']




```python
algo_start_dt
```




    Timestamp('2023-09-21 00:00:00+0000', tz='UTC')




```python
algo = TargetPercentPipeAlgo(
                     start_session=algo_start_dt,
                     end_session=end_dt,           
                     tradeday=tradeday,
                     pipeline=make_pipeline,
)

# set_algo_instance
set_algo_instance(algo)

# run
stats = algo.run()
```

    [2024-03-13 02:41:05.883443]: INFO: handle_simulation_end: Simulated 8 trading days
    first open: 2023-09-21 01:01:00+00:00
    last close: 2023-10-03 05:30:00+00:00
    


```python
positions, transactions, orders = get_transaction_detail(stats)
```


```python
transactions
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>sid</th>
      <th>symbol</th>
      <th>amount</th>
      <th>dt</th>
      <th>price</th>
      <th>order_id</th>
      <th>asset</th>
      <th>commission</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2023-10-03 13:30:00+08:00</th>
      <td>24</td>
      <td>2603</td>
      <td>35242</td>
      <td>2023-10-03 13:30:00+08:00</td>
      <td>110.500051</td>
      <td>35a33e935bde4439b8e113256e10f083</td>
      <td>Equity(24 [2603])</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2023-10-03 13:30:00+08:00</th>
      <td>34</td>
      <td>2886</td>
      <td>106382</td>
      <td>2023-10-03 13:30:00+08:00</td>
      <td>37.650179</td>
      <td>fb0b44482bc54fff9b6dec3fd649e715</td>
      <td>Equity(34 [2886])</td>
      <td>None</td>
    </tr>
  </tbody>
</table>
</div>



<span id="Case4"></span>
## Case 4 調整rebalance_date_rule
[Return to Menu](#menu)  

接續**Case 1**，多新增`rebalance_date_rule`，並修改`start_session`為2023-09-01，其餘與**Case 1**相同。


```python
from zipline.api import date_rules

algo = TargetPercentPipeAlgo(
                     start_session=pd.Timestamp('2023-09-01', tz='UTC'),
                     end_session=end_dt,
                     pipeline=make_pipeline,
                     # 每月的第四個交易日下單
                     rebalance_date_rule=date_rules.month_start(days_offset=3) 
)

# set_algo_instance
set_algo_instance(algo)

# run
stats = algo.run()
```

    [2024-03-13 02:41:06.016104]: INFO: handle_simulation_end: Simulated 22 trading days
    first open: 2023-09-01 01:01:00+00:00
    last close: 2023-10-03 05:30:00+00:00
    


```python
positions, transactions, orders = get_transaction_detail(stats)
```


```python
orders
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>sid</th>
      <th>symbol</th>
      <th>id</th>
      <th>dt</th>
      <th>reason</th>
      <th>created</th>
      <th>amount</th>
      <th>filled</th>
      <th>commission</th>
      <th>stop</th>
      <th>limit</th>
      <th>stop_reached</th>
      <th>limit_reached</th>
      <th>asset</th>
      <th>status</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2023-09-06 13:30:00+08:00</th>
      <td>41</td>
      <td>3034</td>
      <td>4706e48bb8644098b53fd9a9c90ed314</td>
      <td>2023-09-06 13:30:00+08:00</td>
      <td>None</td>
      <td>2023-09-06 13:30:00+08:00</td>
      <td>9411</td>
      <td>0</td>
      <td>0</td>
      <td>None</td>
      <td>None</td>
      <td>False</td>
      <td>False</td>
      <td>Equity(41 [3034])</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2023-09-06 13:30:00+08:00</th>
      <td>14</td>
      <td>2327</td>
      <td>ca20e3e3e6554702a701388075f7b1f3</td>
      <td>2023-09-06 13:30:00+08:00</td>
      <td>None</td>
      <td>2023-09-06 13:30:00+08:00</td>
      <td>7920</td>
      <td>0</td>
      <td>0</td>
      <td>None</td>
      <td>None</td>
      <td>False</td>
      <td>False</td>
      <td>Equity(14 [2327])</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2023-09-07 13:30:00+08:00</th>
      <td>41</td>
      <td>3034</td>
      <td>4706e48bb8644098b53fd9a9c90ed314</td>
      <td>2023-09-07 13:30:00+08:00</td>
      <td>None</td>
      <td>2023-09-06 13:30:00+08:00</td>
      <td>9411</td>
      <td>9411</td>
      <td>5727</td>
      <td>None</td>
      <td>None</td>
      <td>False</td>
      <td>False</td>
      <td>Equity(41 [3034])</td>
      <td>1</td>
    </tr>
    <tr>
      <th>2023-09-07 13:30:00+08:00</th>
      <td>14</td>
      <td>2327</td>
      <td>ca20e3e3e6554702a701388075f7b1f3</td>
      <td>2023-09-07 13:30:00+08:00</td>
      <td>None</td>
      <td>2023-09-06 13:30:00+08:00</td>
      <td>7920</td>
      <td>7920</td>
      <td>5666</td>
      <td>None</td>
      <td>None</td>
      <td>False</td>
      <td>False</td>
      <td>Equity(14 [2327])</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>



<span id="Case5"></span>
## Case 5 調整slippage_model
[Return to Menu](#menu)  

接續**Case 1**，多新增`slippage_model`，將`volume_limit`由**0.025**調整為**0.01**，其餘與**Case 1**相同。


```python
algo = TargetPercentPipeAlgo(
                     start_session=algo_start_dt,
                     end_session=end_dt,
                     pipeline=make_pipeline,
                     slippage_model=slippage.VolumeShareSlippage(volume_limit=0.01, price_impact=0.1)
)

# set_algo_instance
set_algo_instance(algo)

# run
stats = algo.run()
```

    [2024-03-13 02:41:06.113628]: INFO: rebalance: Cancel_order: current time: 2023-09-22 , created: 2023-09-21 , asset: Equity(46 [4904]), amount: 55478 , filled: 20760
    [2024-03-13 02:41:06.121367]: INFO: rebalance: Cancel_order: current time: 2023-09-25 , created: 2023-09-25 , asset: Equity(46 [4904]), amount: -5780 , filled: 0
    [2024-03-13 02:41:06.121367]: INFO: earn_dividends: Equity(6 [1590]), cash_dividend amount: 13.43905496, pay_date: 2023-10-30, div_owed: 54629.7584124
    [2024-03-13 02:41:06.121367]: INFO: handle_split: after split: asset: Equity(6 [1590]), amount: 4063, cost_basis: 982.73, last_sale_price: 981.0
    [2024-03-13 02:41:06.121367]: INFO: handle_split: returning cash: 643.62
    [2024-03-13 02:41:06.132990]: INFO: rebalance: Cancel_order: current time: 2023-09-26 , created: 2023-09-26 , asset: Equity(6 [1590]), amount: -853 , filled: 0
    [2024-03-13 02:41:06.138066]: INFO: rebalance: Cancel_order: current time: 2023-09-27 , created: 2023-09-26 , asset: Equity(33 [2885]), amount: 157400 , filled: 151500
    [2024-03-13 02:41:06.138066]: INFO: rebalance: Cancel_order: current time: 2023-09-28 , created: 2023-09-28 , asset: Equity(33 [2885]), amount: -44120 , filled: 0
    [2024-03-13 02:41:06.171376]: INFO: handle_simulation_end: Simulated 8 trading days
    first open: 2023-09-21 01:01:00+00:00
    last close: 2023-10-03 05:30:00+00:00
    


```python
positions, transactions, orders = get_transaction_detail(stats)
```


```python
orders.query('(symbol == "1590") & (created.dt.strftime("%Y-%m-%d") == "2023-09-25")')
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>sid</th>
      <th>symbol</th>
      <th>id</th>
      <th>dt</th>
      <th>reason</th>
      <th>created</th>
      <th>amount</th>
      <th>filled</th>
      <th>commission</th>
      <th>stop</th>
      <th>limit</th>
      <th>stop_reached</th>
      <th>limit_reached</th>
      <th>asset</th>
      <th>status</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2023-09-25 13:30:00+08:00</th>
      <td>6</td>
      <td>1590</td>
      <td>3217f240f39a4bc88fcdce896268d7cc</td>
      <td>2023-09-25 13:30:00+08:00</td>
      <td>None</td>
      <td>2023-09-25 13:30:00+08:00</td>
      <td>-4065</td>
      <td>0</td>
      <td>0.0</td>
      <td>None</td>
      <td>None</td>
      <td>False</td>
      <td>False</td>
      <td>Equity(6 [1590])</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2023-09-26 13:30:00+08:00</th>
      <td>6</td>
      <td>1590</td>
      <td>3217f240f39a4bc88fcdce896268d7cc</td>
      <td>2023-09-26 13:30:00+08:00</td>
      <td>None</td>
      <td>2023-09-25 13:30:00+08:00</td>
      <td>-4063</td>
      <td>-3210</td>
      <td>13609.0</td>
      <td>None</td>
      <td>None</td>
      <td>False</td>
      <td>False</td>
      <td>Equity(6 [1590])</td>
      <td>2</td>
    </tr>
  </tbody>
</table>
</div>




```python
# 321000 * 1% = 3210(股) 

df_bundle_price.query('(symbol == "1590") & (date.dt.strftime("%Y-%m-%d") == "2023-09-26")')[['symbol','date','volume']]
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>symbol</th>
      <th>date</th>
      <th>volume</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>4486</th>
      <td>1590</td>
      <td>2023-09-26 00:00:00+00:00</td>
      <td>321000.0</td>
    </tr>
  </tbody>
</table>
</div>




```python
orders.query('(symbol == "2885") & (created.dt.strftime("%Y-%m-%d") == "2023-09-27")')
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>sid</th>
      <th>symbol</th>
      <th>id</th>
      <th>dt</th>
      <th>reason</th>
      <th>created</th>
      <th>amount</th>
      <th>filled</th>
      <th>commission</th>
      <th>stop</th>
      <th>limit</th>
      <th>stop_reached</th>
      <th>limit_reached</th>
      <th>asset</th>
      <th>status</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2023-09-27 13:30:00+08:00</th>
      <td>33</td>
      <td>2885</td>
      <td>06063291dd4e449a8871819852919f94</td>
      <td>2023-09-27 13:30:00+08:00</td>
      <td>None</td>
      <td>2023-09-27 13:30:00+08:00</td>
      <td>-151500</td>
      <td>0</td>
      <td>0.0</td>
      <td>None</td>
      <td>None</td>
      <td>False</td>
      <td>False</td>
      <td>Equity(33 [2885])</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2023-09-28 13:30:00+08:00</th>
      <td>33</td>
      <td>2885</td>
      <td>06063291dd4e449a8871819852919f94</td>
      <td>2023-09-28 13:30:00+08:00</td>
      <td>None</td>
      <td>2023-09-27 13:30:00+08:00</td>
      <td>-151500</td>
      <td>-107380</td>
      <td>11904.0</td>
      <td>None</td>
      <td>None</td>
      <td>False</td>
      <td>False</td>
      <td>Equity(33 [2885])</td>
      <td>2</td>
    </tr>
  </tbody>
</table>
</div>




```python
# 10738000 * 1% = 107380(股) 

df_bundle_price.query('(symbol == "2885") & (date.dt.strftime("%Y-%m-%d") == "2023-09-28")')[['symbol','date','volume']]
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>symbol</th>
      <th>date</th>
      <th>volume</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>4625</th>
      <td>2885</td>
      <td>2023-09-28 00:00:00+00:00</td>
      <td>10738000.0</td>
    </tr>
  </tbody>
</table>
</div>



<span id="Case6"></span>
## Case 6 調整stocklist
[Return to Menu](#menu)  


接續**Case 1**，多新增`stocklist`，其餘與**Case 1**相同。  



註1：`stocklist`限制是在pipeline執行完後。  

註2：也可以使用pipeline直接限制股票池。


```python
len(StockList)
```




    55




```python
_StockList = [i for i in StockList if i!='2886']
len(_StockList)
```




    54




```python
algo = TargetPercentPipeAlgo(
                     start_session=algo_start_dt,
                     end_session=end_dt,            
                     stocklist=_StockList,
                     pipeline=make_pipeline,
)

# set_algo_instance
set_algo_instance(algo)

# run
stats = algo.run()
```

    [2024-03-13 02:41:06.339500]: INFO: rebalance: Cancel_order: current time: 2023-09-22 , created: 2023-09-21 , asset: Equity(46 [4904]), amount: 55478 , filled: 51900
    [2024-03-13 02:41:06.346237]: INFO: rebalance: Cancel_order: current time: 2023-09-25 , created: 2023-09-25 , asset: Equity(46 [4904]), amount: -14450 , filled: 0
    [2024-03-13 02:41:06.349619]: INFO: earn_dividends: Equity(6 [1590]), cash_dividend amount: 13.43905496, pay_date: 2023-10-30, div_owed: 54643.19746736
    [2024-03-13 02:41:06.350619]: INFO: handle_split: after split: asset: Equity(6 [1590]), amount: 4064, cost_basis: 982.73, last_sale_price: 981.0
    [2024-03-13 02:41:06.350619]: INFO: handle_split: returning cash: 643.29
    [2024-03-13 02:41:06.385375]: INFO: handle_simulation_end: Simulated 8 trading days
    first open: 2023-09-21 01:01:00+00:00
    last close: 2023-10-03 05:30:00+00:00
    


```python
positions, transactions, orders = get_transaction_detail(stats)
```


```python
positions
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>sid</th>
      <th>symbol</th>
      <th>asset</th>
      <th>amount</th>
      <th>cost_basis</th>
      <th>last_sale_price</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2023-09-22 13:30:00+08:00</th>
      <td>46</td>
      <td>4904</td>
      <td>Equity(46 [4904])</td>
      <td>51900</td>
      <td>72.207262</td>
      <td>72.10</td>
    </tr>
    <tr>
      <th>2023-09-25 13:30:00+08:00</th>
      <td>46</td>
      <td>4904</td>
      <td>Equity(46 [4904])</td>
      <td>14450</td>
      <td>73.043317</td>
      <td>72.90</td>
    </tr>
    <tr>
      <th>2023-09-25 13:30:00+08:00</th>
      <td>24</td>
      <td>2603</td>
      <td>Equity(24 [2603])</td>
      <td>34915</td>
      <td>116.165477</td>
      <td>116.00</td>
    </tr>
    <tr>
      <th>2023-09-25 13:30:00+08:00</th>
      <td>6</td>
      <td>1590</td>
      <td>Equity(6 [1590])</td>
      <td>4066</td>
      <td>982.407159</td>
      <td>981.00</td>
    </tr>
    <tr>
      <th>2023-09-26 13:30:00+08:00</th>
      <td>25</td>
      <td>2609</td>
      <td>Equity(25 [2609])</td>
      <td>87157</td>
      <td>46.015867</td>
      <td>45.95</td>
    </tr>
    <tr>
      <th>2023-09-27 13:30:00+08:00</th>
      <td>33</td>
      <td>2885</td>
      <td>Equity(33 [2885])</td>
      <td>157624</td>
      <td>25.186117</td>
      <td>25.15</td>
    </tr>
    <tr>
      <th>2023-09-28 13:30:00+08:00</th>
      <td>25</td>
      <td>2609</td>
      <td>Equity(25 [2609])</td>
      <td>85610</td>
      <td>45.515095</td>
      <td>45.45</td>
    </tr>
    <tr>
      <th>2023-10-02 13:30:00+08:00</th>
      <td>6</td>
      <td>1590</td>
      <td>Equity(6 [1590])</td>
      <td>3997</td>
      <td>1006.440770</td>
      <td>1005.00</td>
    </tr>
    <tr>
      <th>2023-10-03 13:30:00+08:00</th>
      <td>24</td>
      <td>2603</td>
      <td>Equity(24 [2603])</td>
      <td>34102</td>
      <td>110.657517</td>
      <td>110.50</td>
    </tr>
  </tbody>
</table>
</div>



<span id="Case7"></span>
## Case 7 調整order_filling_policy
[Return to Menu](#menu) 

接續**Case 1**，多新增`order_filling_policy='current_bar'`，其餘與**Case 1**相同。


```python
algo = TargetPercentPipeAlgo(
                     start_session=algo_start_dt,
                     end_session=end_dt,
                     pipeline=make_pipeline,
                     order_filling_policy='current_bar'
)

# set_algo_instance
set_algo_instance(algo)

# run
stats = algo.run()
```

    [2024-03-13 02:41:06.476428]: INFO: rebalance: Cancel_order: current time: 2023-09-25 , created: 2023-09-25 , asset: Equity(46 [4904]), amount: -3578 , filled: 0
    [2024-03-13 02:41:06.523514]: INFO: handle_simulation_end: Simulated 8 trading days
    first open: 2023-09-21 01:01:00+00:00
    last close: 2023-10-03 05:30:00+00:00
    


```python
positions, transactions, orders = get_transaction_detail(stats)
```


```python
result.loc['2023-09-22'].query('longs == True')
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>longs</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>Equity(6 [1590])</th>
      <td>True</td>
    </tr>
    <tr>
      <th>Equity(24 [2603])</th>
      <td>True</td>
    </tr>
  </tbody>
</table>
</div>




```python
# 從`orders`中可以發現created=2023-09-22的單在當天就成交（status由0變為1）
orders.loc['2023-09-22']
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>sid</th>
      <th>symbol</th>
      <th>id</th>
      <th>dt</th>
      <th>reason</th>
      <th>created</th>
      <th>amount</th>
      <th>filled</th>
      <th>commission</th>
      <th>stop</th>
      <th>limit</th>
      <th>stop_reached</th>
      <th>limit_reached</th>
      <th>asset</th>
      <th>status</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2023-09-22 13:30:00+08:00</th>
      <td>34</td>
      <td>2886</td>
      <td>36e7f2eb3d37492e829558f5748560aa</td>
      <td>2023-09-22 13:30:00+08:00</td>
      <td>None</td>
      <td>2023-09-22 08:45:00+08:00</td>
      <td>-106951</td>
      <td>-106951</td>
      <td>17890.0</td>
      <td>None</td>
      <td>None</td>
      <td>False</td>
      <td>False</td>
      <td>Equity(34 [2886])</td>
      <td>1</td>
    </tr>
    <tr>
      <th>2023-09-22 13:30:00+08:00</th>
      <td>46</td>
      <td>4904</td>
      <td>0fc833a0c418408dae3da988454b2029</td>
      <td>2023-09-22 13:30:00+08:00</td>
      <td>None</td>
      <td>2023-09-22 08:45:00+08:00</td>
      <td>-55478</td>
      <td>-51900</td>
      <td>16559.0</td>
      <td>None</td>
      <td>None</td>
      <td>False</td>
      <td>False</td>
      <td>Equity(46 [4904])</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2023-09-22 13:30:00+08:00</th>
      <td>24</td>
      <td>2603</td>
      <td>737aa5d17c9c43ff814b7c09a94898fe</td>
      <td>2023-09-22 13:30:00+08:00</td>
      <td>None</td>
      <td>2023-09-22 08:45:00+08:00</td>
      <td>35043</td>
      <td>35043</td>
      <td>5718.0</td>
      <td>None</td>
      <td>None</td>
      <td>False</td>
      <td>False</td>
      <td>Equity(24 [2603])</td>
      <td>1</td>
    </tr>
    <tr>
      <th>2023-09-22 13:30:00+08:00</th>
      <td>6</td>
      <td>1590</td>
      <td>50897cf4326c46abbea7efdf8869a5af</td>
      <td>2023-09-22 13:30:00+08:00</td>
      <td>None</td>
      <td>2023-09-22 08:45:00+08:00</td>
      <td>4081</td>
      <td>4081</td>
      <td>5717.0</td>
      <td>None</td>
      <td>None</td>
      <td>False</td>
      <td>False</td>
      <td>Equity(6 [1590])</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>



<span id="Case8"></span>
## Case 8 調整allow_short
[Return to Menu](#menu) 

接續**Case 1**，多新增`allow_short=True`，其餘與**Case 1**相同。  


以下設定pipeline（`make_pipeline()`），並定義`shorts`欄位用來判斷須放空的股票。在`shorts`欄位中要放空的股票標記為True，反之標記為False。


```python
def make_pipeline():
    rsi = RSI()
    longs = rsi.top(2, mask = ~SingleAsset(bundle.asset_finder.lookup_symbol('IR0001', as_of_date=None)))
    shorts = rsi.bottom(2, mask = ~SingleAsset(bundle.asset_finder.lookup_symbol('IR0001', as_of_date=None)))

    return Pipeline(
        columns = {
            "longs" : longs,
            "shorts" : shorts
        }
    )
```


```python
result = run_pipeline(make_pipeline(), algo_start, end)
result.query('(longs == True) | (shorts == True)' )
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th></th>
      <th>longs</th>
      <th>shorts</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="4" valign="top">2023-09-22 00:00:00+00:00</th>
      <th>Equity(6 [1590])</th>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th>Equity(10 [2301])</th>
      <td>False</td>
      <td>True</td>
    </tr>
    <tr>
      <th>Equity(24 [2603])</th>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th>Equity(39 [2912])</th>
      <td>False</td>
      <td>True</td>
    </tr>
    <tr>
      <th rowspan="4" valign="top">2023-09-25 00:00:00+00:00</th>
      <th>Equity(10 [2301])</th>
      <td>False</td>
      <td>True</td>
    </tr>
    <tr>
      <th>Equity(25 [2609])</th>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th>Equity(31 [2883])</th>
      <td>False</td>
      <td>True</td>
    </tr>
    <tr>
      <th>Equity(34 [2886])</th>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th rowspan="4" valign="top">2023-09-26 00:00:00+00:00</th>
      <th>Equity(8 [2002])</th>
      <td>False</td>
      <td>True</td>
    </tr>
    <tr>
      <th>Equity(10 [2301])</th>
      <td>False</td>
      <td>True</td>
    </tr>
    <tr>
      <th>Equity(33 [2885])</th>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th>Equity(34 [2886])</th>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th rowspan="4" valign="top">2023-09-27 00:00:00+00:00</th>
      <th>Equity(10 [2301])</th>
      <td>False</td>
      <td>True</td>
    </tr>
    <tr>
      <th>Equity(17 [2357])</th>
      <td>False</td>
      <td>True</td>
    </tr>
    <tr>
      <th>Equity(25 [2609])</th>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th>Equity(34 [2886])</th>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th rowspan="4" valign="top">2023-09-28 00:00:00+00:00</th>
      <th>Equity(6 [1590])</th>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th>Equity(10 [2301])</th>
      <td>False</td>
      <td>True</td>
    </tr>
    <tr>
      <th>Equity(34 [2886])</th>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th>Equity(39 [2912])</th>
      <td>False</td>
      <td>True</td>
    </tr>
    <tr>
      <th rowspan="4" valign="top">2023-10-02 00:00:00+00:00</th>
      <th>Equity(10 [2301])</th>
      <td>False</td>
      <td>True</td>
    </tr>
    <tr>
      <th>Equity(24 [2603])</th>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th>Equity(34 [2886])</th>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th>Equity(39 [2912])</th>
      <td>False</td>
      <td>True</td>
    </tr>
    <tr>
      <th rowspan="4" valign="top">2023-10-03 00:00:00+00:00</th>
      <th>Equity(2 [1301])</th>
      <td>False</td>
      <td>True</td>
    </tr>
    <tr>
      <th>Equity(6 [1590])</th>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th>Equity(14 [2327])</th>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th>Equity(30 [2882])</th>
      <td>False</td>
      <td>True</td>
    </tr>
  </tbody>
</table>
</div>




```python
algo = TargetPercentPipeAlgo(
                     start_session=algo_start_dt,
                     end_session=end_dt,
                     allow_short=True,
                     pipeline=make_pipeline,
)

# set_algo_instance
set_algo_instance(algo)

# run
stats = algo.run()
```

    [2024-03-13 02:41:06.669320]: INFO: rebalance: Cancel_order: current time: 2023-09-22 , created: 2023-09-21 , asset: Equity(46 [4904]), amount: 55478 , filled: 51900
    [2024-03-13 02:41:06.681121]: INFO: rebalance: Cancel_order: current time: 2023-09-25 , created: 2023-09-25 , asset: Equity(46 [4904]), amount: -14450 , filled: 0
    [2024-03-13 02:41:06.687058]: INFO: earn_dividends: Equity(6 [1590]), cash_dividend amount: 13.43905496, pay_date: 2023-10-30, div_owed: 54414.73353304
    [2024-03-13 02:41:06.687453]: INFO: handle_split: after split: asset: Equity(6 [1590]), amount: 4047, cost_basis: 982.73, last_sale_price: 981.0
    [2024-03-13 02:41:06.687453]: INFO: handle_split: returning cash: 648.82
    [2024-03-13 02:41:06.734225]: INFO: rebalance: Cancel_order: current time: 2023-10-02 , created: 2023-09-28 , asset: Equity(39 [2912]), amount: -14669 , filled: -8575
    [2024-03-13 02:41:06.747551]: INFO: handle_simulation_end: Simulated 8 trading days
    first open: 2023-09-21 01:01:00+00:00
    last close: 2023-10-03 05:30:00+00:00
    


```python
positions, transactions, orders = get_transaction_detail(stats)
```


```python
# 當天取消
orders.query('(symbol == "2912") & (created.dt.strftime("%Y-%m-%d") == "2023-09-28")')
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>sid</th>
      <th>symbol</th>
      <th>id</th>
      <th>dt</th>
      <th>reason</th>
      <th>created</th>
      <th>amount</th>
      <th>filled</th>
      <th>commission</th>
      <th>stop</th>
      <th>limit</th>
      <th>stop_reached</th>
      <th>limit_reached</th>
      <th>asset</th>
      <th>status</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2023-09-28 13:30:00+08:00</th>
      <td>39</td>
      <td>2912</td>
      <td>2ed71c04d41546fabc89536cab7b49de</td>
      <td>2023-09-28 13:30:00+08:00</td>
      <td>None</td>
      <td>2023-09-28 13:30:00+08:00</td>
      <td>-14669</td>
      <td>0</td>
      <td>0.0</td>
      <td>None</td>
      <td>None</td>
      <td>False</td>
      <td>False</td>
      <td>Equity(39 [2912])</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2023-10-02 13:30:00+08:00</th>
      <td>39</td>
      <td>2912</td>
      <td>2ed71c04d41546fabc89536cab7b49de</td>
      <td>2023-10-02 13:30:00+08:00</td>
      <td>None</td>
      <td>2023-09-28 13:30:00+08:00</td>
      <td>-14669</td>
      <td>-8575</td>
      <td>9980.0</td>
      <td>None</td>
      <td>None</td>
      <td>False</td>
      <td>False</td>
      <td>Equity(39 [2912])</td>
      <td>2</td>
    </tr>
  </tbody>
</table>
</div>




```python
# 343000 * 2.5% = 8575(股) 

df_bundle_price.query('(symbol == "2912") & (date.dt.strftime("%Y-%m-%d") == "2023-10-02")')[['symbol','date','volume']]
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>symbol</th>
      <th>date</th>
      <th>volume</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>4687</th>
      <td>2912</td>
      <td>2023-10-02 00:00:00+00:00</td>
      <td>343000.0</td>
    </tr>
  </tbody>
</table>
</div>




```python
positions['mv'] = positions['amount'] * positions['last_sale_price']
positions.query('(symbol == "2912")')
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>sid</th>
      <th>symbol</th>
      <th>asset</th>
      <th>amount</th>
      <th>cost_basis</th>
      <th>last_sale_price</th>
      <th>mv</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2023-09-25 13:30:00+08:00</th>
      <td>39</td>
      <td>2912</td>
      <td>Equity(39 [2912])</td>
      <td>-15079</td>
      <td>263.315631</td>
      <td>264.5</td>
      <td>-3988395.5</td>
    </tr>
    <tr>
      <th>2023-10-02 13:30:00+08:00</th>
      <td>39</td>
      <td>2912</td>
      <td>Equity(39 [2912])</td>
      <td>-8575</td>
      <td>261.819714</td>
      <td>263.0</td>
      <td>-2255225.0</td>
    </tr>
    <tr>
      <th>2023-10-03 13:30:00+08:00</th>
      <td>39</td>
      <td>2912</td>
      <td>Equity(39 [2912])</td>
      <td>-14152</td>
      <td>261.433302</td>
      <td>262.0</td>
      <td>-3707824.0</td>
    </tr>
  </tbody>
</table>
</div>



<span id="Case9"></span>
## Case 9 調整cancel_datedelta
[Return to Menu](#menu) 

接續**Case 8**，多新增`cancel_datedelta=2`，其餘與**Case 8**相同。  


```python
algo = TargetPercentPipeAlgo(
                     start_session=algo_start_dt,
                     end_session=end_dt,
                     allow_short=True,
                     cancel_datedelta=2,
                     pipeline=make_pipeline,
)

# set_algo_instance
set_algo_instance(algo)

# run
stats = algo.run()
```

    [2024-03-13 02:41:06.883576]: INFO: earn_dividends: Equity(6 [1590]), cash_dividend amount: 13.43905496, pay_date: 2023-10-30, div_owed: 54414.73353304
    [2024-03-13 02:41:06.883576]: INFO: handle_split: after split: asset: Equity(6 [1590]), amount: 4047, cost_basis: 982.73, last_sale_price: 981.0
    [2024-03-13 02:41:06.883576]: INFO: handle_split: returning cash: 648.82
    [2024-03-13 02:41:06.951237]: INFO: handle_simulation_end: Simulated 8 trading days
    first open: 2023-09-21 01:01:00+00:00
    last close: 2023-10-03 05:30:00+00:00
    


```python
result = run_pipeline(make_pipeline(), algo_start, end)
result.query('(longs == True) | (shorts == True)' )
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th></th>
      <th>longs</th>
      <th>shorts</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="4" valign="top">2023-09-22 00:00:00+00:00</th>
      <th>Equity(6 [1590])</th>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th>Equity(10 [2301])</th>
      <td>False</td>
      <td>True</td>
    </tr>
    <tr>
      <th>Equity(24 [2603])</th>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th>Equity(39 [2912])</th>
      <td>False</td>
      <td>True</td>
    </tr>
    <tr>
      <th rowspan="4" valign="top">2023-09-25 00:00:00+00:00</th>
      <th>Equity(10 [2301])</th>
      <td>False</td>
      <td>True</td>
    </tr>
    <tr>
      <th>Equity(25 [2609])</th>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th>Equity(31 [2883])</th>
      <td>False</td>
      <td>True</td>
    </tr>
    <tr>
      <th>Equity(34 [2886])</th>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th rowspan="4" valign="top">2023-09-26 00:00:00+00:00</th>
      <th>Equity(8 [2002])</th>
      <td>False</td>
      <td>True</td>
    </tr>
    <tr>
      <th>Equity(10 [2301])</th>
      <td>False</td>
      <td>True</td>
    </tr>
    <tr>
      <th>Equity(33 [2885])</th>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th>Equity(34 [2886])</th>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th rowspan="4" valign="top">2023-09-27 00:00:00+00:00</th>
      <th>Equity(10 [2301])</th>
      <td>False</td>
      <td>True</td>
    </tr>
    <tr>
      <th>Equity(17 [2357])</th>
      <td>False</td>
      <td>True</td>
    </tr>
    <tr>
      <th>Equity(25 [2609])</th>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th>Equity(34 [2886])</th>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th rowspan="4" valign="top">2023-09-28 00:00:00+00:00</th>
      <th>Equity(6 [1590])</th>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th>Equity(10 [2301])</th>
      <td>False</td>
      <td>True</td>
    </tr>
    <tr>
      <th>Equity(34 [2886])</th>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th>Equity(39 [2912])</th>
      <td>False</td>
      <td>True</td>
    </tr>
    <tr>
      <th rowspan="4" valign="top">2023-10-02 00:00:00+00:00</th>
      <th>Equity(10 [2301])</th>
      <td>False</td>
      <td>True</td>
    </tr>
    <tr>
      <th>Equity(24 [2603])</th>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th>Equity(34 [2886])</th>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th>Equity(39 [2912])</th>
      <td>False</td>
      <td>True</td>
    </tr>
    <tr>
      <th rowspan="4" valign="top">2023-10-03 00:00:00+00:00</th>
      <th>Equity(2 [1301])</th>
      <td>False</td>
      <td>True</td>
    </tr>
    <tr>
      <th>Equity(6 [1590])</th>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th>Equity(14 [2327])</th>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th>Equity(30 [2882])</th>
      <td>False</td>
      <td>True</td>
    </tr>
  </tbody>
</table>
</div>




```python
positions, transactions, orders = get_transaction_detail(stats)
```


```python
orders.query('(symbol == "2912") & (created.dt.strftime("%Y-%m-%d") == "2023-09-28")')
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>sid</th>
      <th>symbol</th>
      <th>id</th>
      <th>dt</th>
      <th>reason</th>
      <th>created</th>
      <th>amount</th>
      <th>filled</th>
      <th>commission</th>
      <th>stop</th>
      <th>limit</th>
      <th>stop_reached</th>
      <th>limit_reached</th>
      <th>asset</th>
      <th>status</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2023-09-28 13:30:00+08:00</th>
      <td>39</td>
      <td>2912</td>
      <td>05ab5284f0904f0599fce02cafe7dd93</td>
      <td>2023-09-28 13:30:00+08:00</td>
      <td>None</td>
      <td>2023-09-28 13:30:00+08:00</td>
      <td>-14660</td>
      <td>0</td>
      <td>0.0</td>
      <td>None</td>
      <td>None</td>
      <td>False</td>
      <td>False</td>
      <td>Equity(39 [2912])</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2023-10-02 13:30:00+08:00</th>
      <td>39</td>
      <td>2912</td>
      <td>05ab5284f0904f0599fce02cafe7dd93</td>
      <td>2023-10-02 13:30:00+08:00</td>
      <td>None</td>
      <td>2023-09-28 13:30:00+08:00</td>
      <td>-14660</td>
      <td>-8575</td>
      <td>9980.0</td>
      <td>None</td>
      <td>None</td>
      <td>False</td>
      <td>False</td>
      <td>Equity(39 [2912])</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2023-10-03 13:30:00+08:00</th>
      <td>39</td>
      <td>2912</td>
      <td>05ab5284f0904f0599fce02cafe7dd93</td>
      <td>2023-10-03 13:30:00+08:00</td>
      <td>None</td>
      <td>2023-09-28 13:30:00+08:00</td>
      <td>-14660</td>
      <td>-14660</td>
      <td>17035.0</td>
      <td>None</td>
      <td>None</td>
      <td>False</td>
      <td>False</td>
      <td>Equity(39 [2912])</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>




```python
# 10/02：343000 * 2.5% = 8575(股) 
# 10/03：808000 * 2.5% = 20200(股) 

df_bundle_price.query('(symbol == "2912") & (date.dt.strftime("%Y-%m-%d")>="2023-10-02")')\
                 [['symbol','date','volume']]
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>symbol</th>
      <th>date</th>
      <th>volume</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>4687</th>
      <td>2912</td>
      <td>2023-10-02 00:00:00+00:00</td>
      <td>343000.0</td>
    </tr>
    <tr>
      <th>4743</th>
      <td>2912</td>
      <td>2023-10-03 00:00:00+00:00</td>
      <td>808000.0</td>
    </tr>
  </tbody>
</table>
</div>




```python
positions['mv'] = positions['amount'] * positions['last_sale_price']
positions.query('(symbol == "2912")')
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>sid</th>
      <th>symbol</th>
      <th>asset</th>
      <th>amount</th>
      <th>cost_basis</th>
      <th>last_sale_price</th>
      <th>mv</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2023-09-25 13:30:00+08:00</th>
      <td>39</td>
      <td>2912</td>
      <td>Equity(39 [2912])</td>
      <td>-15079</td>
      <td>263.315631</td>
      <td>264.5</td>
      <td>-3988395.5</td>
    </tr>
    <tr>
      <th>2023-10-02 13:30:00+08:00</th>
      <td>39</td>
      <td>2912</td>
      <td>Equity(39 [2912])</td>
      <td>-8575</td>
      <td>261.819714</td>
      <td>263.0</td>
      <td>-2255225.0</td>
    </tr>
    <tr>
      <th>2023-10-03 13:30:00+08:00</th>
      <td>39</td>
      <td>2912</td>
      <td>Equity(39 [2912])</td>
      <td>-14660</td>
      <td>261.412688</td>
      <td>262.0</td>
      <td>-3840920.0</td>
    </tr>
  </tbody>
</table>
</div>



<span id="Case10"></span>
## Case 10 調整limit_buy_multiplier
[Return to Menu](#menu) 

接續**Case 9**，多設定`limit_buy_multiplier=1.015`，其餘與**Case 9**相同。


```python
result = run_pipeline(make_pipeline(), algo_start, end)
result.query('(longs == True) | (shorts == True)' )
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th></th>
      <th>longs</th>
      <th>shorts</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="4" valign="top">2023-09-22 00:00:00+00:00</th>
      <th>Equity(6 [1590])</th>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th>Equity(10 [2301])</th>
      <td>False</td>
      <td>True</td>
    </tr>
    <tr>
      <th>Equity(24 [2603])</th>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th>Equity(39 [2912])</th>
      <td>False</td>
      <td>True</td>
    </tr>
    <tr>
      <th rowspan="4" valign="top">2023-09-25 00:00:00+00:00</th>
      <th>Equity(10 [2301])</th>
      <td>False</td>
      <td>True</td>
    </tr>
    <tr>
      <th>Equity(25 [2609])</th>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th>Equity(31 [2883])</th>
      <td>False</td>
      <td>True</td>
    </tr>
    <tr>
      <th>Equity(34 [2886])</th>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th rowspan="4" valign="top">2023-09-26 00:00:00+00:00</th>
      <th>Equity(8 [2002])</th>
      <td>False</td>
      <td>True</td>
    </tr>
    <tr>
      <th>Equity(10 [2301])</th>
      <td>False</td>
      <td>True</td>
    </tr>
    <tr>
      <th>Equity(33 [2885])</th>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th>Equity(34 [2886])</th>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th rowspan="4" valign="top">2023-09-27 00:00:00+00:00</th>
      <th>Equity(10 [2301])</th>
      <td>False</td>
      <td>True</td>
    </tr>
    <tr>
      <th>Equity(17 [2357])</th>
      <td>False</td>
      <td>True</td>
    </tr>
    <tr>
      <th>Equity(25 [2609])</th>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th>Equity(34 [2886])</th>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th rowspan="4" valign="top">2023-09-28 00:00:00+00:00</th>
      <th>Equity(6 [1590])</th>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th>Equity(10 [2301])</th>
      <td>False</td>
      <td>True</td>
    </tr>
    <tr>
      <th>Equity(34 [2886])</th>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th>Equity(39 [2912])</th>
      <td>False</td>
      <td>True</td>
    </tr>
    <tr>
      <th rowspan="4" valign="top">2023-10-02 00:00:00+00:00</th>
      <th>Equity(10 [2301])</th>
      <td>False</td>
      <td>True</td>
    </tr>
    <tr>
      <th>Equity(24 [2603])</th>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th>Equity(34 [2886])</th>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th>Equity(39 [2912])</th>
      <td>False</td>
      <td>True</td>
    </tr>
    <tr>
      <th rowspan="4" valign="top">2023-10-03 00:00:00+00:00</th>
      <th>Equity(2 [1301])</th>
      <td>False</td>
      <td>True</td>
    </tr>
    <tr>
      <th>Equity(6 [1590])</th>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th>Equity(14 [2327])</th>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th>Equity(30 [2882])</th>
      <td>False</td>
      <td>True</td>
    </tr>
  </tbody>
</table>
</div>




```python
algo = TargetPercentPipeAlgo(
                     start_session=algo_start_dt,
                     end_session=end_dt,
                     limit_buy_multiplier=1.015,
                     allow_short=True,
                     cancel_datedelta=2,
                     pipeline=make_pipeline,
)

# set_algo_instance
set_algo_instance(algo)

# run
stats = algo.run()
```

    [2024-03-13 02:41:07.183718]: INFO: earn_dividends: Equity(6 [1590]), cash_dividend amount: 13.43905496, pay_date: 2023-10-30, div_owed: 54414.73353304
    [2024-03-13 02:41:07.183718]: INFO: handle_split: after split: asset: Equity(6 [1590]), amount: 4047, cost_basis: 982.73, last_sale_price: 981.0
    [2024-03-13 02:41:07.183718]: INFO: handle_split: returning cash: 648.82
    [2024-03-13 02:41:07.254114]: INFO: exec_cancel: Cancel_order: current time: 2023-10-03,
                                  due to : created>=current time + cancel_datedelta(2 days),
                                  created: 2023-09-28, asset: Equity(6 [1590]), amount: 3931, filled: 0'
    [2024-03-13 02:41:07.259895]: INFO: handle_simulation_end: Simulated 8 trading days
    first open: 2023-09-21 01:01:00+00:00
    last close: 2023-10-03 05:30:00+00:00
    


```python
positions, transactions, orders = get_transaction_detail(stats)
```


```python
orders
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>sid</th>
      <th>symbol</th>
      <th>id</th>
      <th>dt</th>
      <th>reason</th>
      <th>created</th>
      <th>amount</th>
      <th>filled</th>
      <th>commission</th>
      <th>stop</th>
      <th>limit</th>
      <th>stop_reached</th>
      <th>limit_reached</th>
      <th>asset</th>
      <th>status</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2023-09-21 13:30:00+08:00</th>
      <td>34</td>
      <td>2886</td>
      <td>5e4ce9d6633d408a9389b175c652626b</td>
      <td>2023-09-21 13:30:00+08:00</td>
      <td>None</td>
      <td>2023-09-21 13:30:00+08:00</td>
      <td>106951</td>
      <td>0</td>
      <td>0.0</td>
      <td>None</td>
      <td>37.96</td>
      <td>False</td>
      <td>False</td>
      <td>Equity(34 [2886])</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2023-09-21 13:30:00+08:00</th>
      <td>46</td>
      <td>4904</td>
      <td>e0b2c8d022ff41959b6f077bccad0c43</td>
      <td>2023-09-21 13:30:00+08:00</td>
      <td>None</td>
      <td>2023-09-21 13:30:00+08:00</td>
      <td>55478</td>
      <td>0</td>
      <td>0.0</td>
      <td>None</td>
      <td>73.18</td>
      <td>False</td>
      <td>False</td>
      <td>Equity(46 [4904])</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2023-09-21 13:30:00+08:00</th>
      <td>10</td>
      <td>2301</td>
      <td>25b053b3403b4ec4aa3408745f592ac4</td>
      <td>2023-09-21 13:30:00+08:00</td>
      <td>None</td>
      <td>2023-09-21 13:30:00+08:00</td>
      <td>-33613</td>
      <td>0</td>
      <td>0.0</td>
      <td>None</td>
      <td>NaN</td>
      <td>False</td>
      <td>False</td>
      <td>Equity(10 [2301])</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2023-09-21 13:30:00+08:00</th>
      <td>44</td>
      <td>3231</td>
      <td>7a065d9e8d604f699ec3efd6ca41d47c</td>
      <td>2023-09-21 13:30:00+08:00</td>
      <td>None</td>
      <td>2023-09-21 13:30:00+08:00</td>
      <td>-39800</td>
      <td>0</td>
      <td>0.0</td>
      <td>None</td>
      <td>NaN</td>
      <td>False</td>
      <td>False</td>
      <td>Equity(44 [3231])</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2023-09-22 13:30:00+08:00</th>
      <td>34</td>
      <td>2886</td>
      <td>5e4ce9d6633d408a9389b175c652626b</td>
      <td>2023-09-22 13:30:00+08:00</td>
      <td>None</td>
      <td>2023-09-21 13:30:00+08:00</td>
      <td>106951</td>
      <td>106951</td>
      <td>5761.0</td>
      <td>None</td>
      <td>37.96</td>
      <td>False</td>
      <td>True</td>
      <td>Equity(34 [2886])</td>
      <td>1</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>2023-10-03 13:30:00+08:00</th>
      <td>39</td>
      <td>2912</td>
      <td>aaa092b0d9954807a887fab5edbed96e</td>
      <td>2023-10-03 13:30:00+08:00</td>
      <td>None</td>
      <td>2023-10-03 13:30:00+08:00</td>
      <td>14660</td>
      <td>0</td>
      <td>0.0</td>
      <td>None</td>
      <td>NaN</td>
      <td>False</td>
      <td>False</td>
      <td>Equity(39 [2912])</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2023-10-03 13:30:00+08:00</th>
      <td>6</td>
      <td>1590</td>
      <td>01f05fe434e643268c1562ae1004734a</td>
      <td>2023-10-03 13:30:00+08:00</td>
      <td>None</td>
      <td>2023-10-03 13:30:00+08:00</td>
      <td>3762</td>
      <td>0</td>
      <td>0.0</td>
      <td>None</td>
      <td>1012.97</td>
      <td>False</td>
      <td>False</td>
      <td>Equity(6 [1590])</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2023-10-03 13:30:00+08:00</th>
      <td>14</td>
      <td>2327</td>
      <td>606f2de8c38d4be484df5d6889fe5e28</td>
      <td>2023-10-03 13:30:00+08:00</td>
      <td>None</td>
      <td>2023-10-03 13:30:00+08:00</td>
      <td>7221</td>
      <td>0</td>
      <td>0.0</td>
      <td>None</td>
      <td>527.80</td>
      <td>False</td>
      <td>False</td>
      <td>Equity(14 [2327])</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2023-10-03 13:30:00+08:00</th>
      <td>2</td>
      <td>1301</td>
      <td>d5aa6f02906f4e008bceac04ade72d21</td>
      <td>2023-10-03 13:30:00+08:00</td>
      <td>None</td>
      <td>2023-10-03 13:30:00+08:00</td>
      <td>-47471</td>
      <td>0</td>
      <td>0.0</td>
      <td>None</td>
      <td>NaN</td>
      <td>False</td>
      <td>False</td>
      <td>Equity(2 [1301])</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2023-10-03 13:30:00+08:00</th>
      <td>30</td>
      <td>2882</td>
      <td>82adef75477d4095beaf6b4e02657dcb</td>
      <td>2023-10-03 13:30:00+08:00</td>
      <td>None</td>
      <td>2023-10-03 13:30:00+08:00</td>
      <td>-85340</td>
      <td>0</td>
      <td>0.0</td>
      <td>None</td>
      <td>NaN</td>
      <td>False</td>
      <td>False</td>
      <td>Equity(30 [2882])</td>
      <td>0</td>
    </tr>
  </tbody>
</table>
<p>92 rows × 15 columns</p>
</div>




```python
orders.query('(symbol == "1590") & (created.dt.strftime("%Y-%m-%d") == "2023-09-28")')
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>sid</th>
      <th>symbol</th>
      <th>id</th>
      <th>dt</th>
      <th>reason</th>
      <th>created</th>
      <th>amount</th>
      <th>filled</th>
      <th>commission</th>
      <th>stop</th>
      <th>limit</th>
      <th>stop_reached</th>
      <th>limit_reached</th>
      <th>asset</th>
      <th>status</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2023-09-28 13:30:00+08:00</th>
      <td>6</td>
      <td>1590</td>
      <td>7a1633e9d2314561bf5fc6b235784201</td>
      <td>2023-09-28 13:30:00+08:00</td>
      <td>None</td>
      <td>2023-09-28 13:30:00+08:00</td>
      <td>3931</td>
      <td>0</td>
      <td>0.0</td>
      <td>None</td>
      <td>993.68</td>
      <td>False</td>
      <td>False</td>
      <td>Equity(6 [1590])</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2023-10-03 13:30:00+08:00</th>
      <td>6</td>
      <td>1590</td>
      <td>7a1633e9d2314561bf5fc6b235784201</td>
      <td>2023-10-03 13:30:00+08:00</td>
      <td>None</td>
      <td>2023-09-28 13:30:00+08:00</td>
      <td>3931</td>
      <td>0</td>
      <td>0.0</td>
      <td>None</td>
      <td>993.68</td>
      <td>False</td>
      <td>False</td>
      <td>Equity(6 [1590])</td>
      <td>2</td>
    </tr>
  </tbody>
</table>
</div>




```python
# 9/28 979 * 1.015 = 993.685
df_bundle_price.query('(symbol == "1590") & (date.dt.strftime("%Y-%m-%d")>="2023-09-28")')\
                 [['symbol','date','close']]
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>symbol</th>
      <th>date</th>
      <th>close</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>4598</th>
      <td>1590</td>
      <td>2023-09-28 00:00:00+00:00</td>
      <td>979.0</td>
    </tr>
    <tr>
      <th>4654</th>
      <td>1590</td>
      <td>2023-10-02 00:00:00+00:00</td>
      <td>1005.0</td>
    </tr>
    <tr>
      <th>4710</th>
      <td>1590</td>
      <td>2023-10-03 00:00:00+00:00</td>
      <td>998.0</td>
    </tr>
  </tbody>
</table>
</div>



<span id="Case11"></span>
## Case 11 調整custom_weight、analyze、record_vars、get_record_vars與get_transaction_detail
[Return to Menu](#menu) 

接續**Case 10**，多設定`custom_weight`=True、`analyze`、`record_vars`、`get_record_vars`=True與`get_transaction_detail`=True，其餘與**Case 10**相同。


```python
class Weight(CustomFactor):
    
    inputs =  [TQAltDataSet.Market_Cap_Dollars] 
    outputs = ["Market_Cap_Dollars","Sum_Market_Cap_Dollars","Weight"]   
    window_length = 1

    def compute(self, today, assets, out, Market_Cap_Dollars):
        
        out.Market_Cap_Dollars[:] = Market_Cap_Dollars
        out.Sum_Market_Cap_Dollars[:] = np.nansum(Market_Cap_Dollars, axis=1)
        out.Weight[:] = Market_Cap_Dollars / np.sum(Market_Cap_Dollars, axis=1)
```


```python
def make_pipeline():
    
    rsi = RSI()
    longs = rsi.top(2, mask = ~SingleAsset(bundle.asset_finder.lookup_symbol('IR0001', as_of_date=None)))
    shorts = rsi.bottom(2, mask = ~SingleAsset(bundle.asset_finder.lookup_symbol('IR0001', as_of_date=None)))

    return Pipeline(
        
        columns = {
            "Market_Cap_Dollars":Weight().Market_Cap_Dollars,        
            "longs" : longs,
            "shorts" : shorts,
            "long_weights" : Weight(mask=longs).Weight,
            "short_weights" : Weight(mask=shorts).Weight
        }
    )
```


```python
def analyze(context, perf):
    
    fig = plt.figure(figsize=(16, 24), dpi=400)
    
    # First chart(累積報酬)
    ax = fig.add_subplot(611) 
    ax.set_title('Strategy Results') 
    ax.plot(perf['algorithm_period_return'],
            linestyle='-', 
            label='algorithm period return',
            linewidth=3.0)
    ax.plot(perf['benchmark_period_return'],
            linestyle='-', 
            label='benchmark period return',
            linewidth=3.0)
    ax.legend()
    ax.grid(False)
    
    # Second chart(returns)
    ax = fig.add_subplot(612, sharex=ax)       
    ax.plot(perf['returns'],
            linestyle='-', 
            label='returns',
            linewidth=3.0)
    ax.legend()
    ax.grid(False)

    # Third chart(ending_cash) -> 觀察是否超買
    ax = fig.add_subplot(613, sharex=ax)
    ax.plot(perf['ending_cash'], 
            label='ending_cash',
            linestyle='-',
            linewidth=3.0)
    ax.axhline(y=1, c='r', linewidth=1)
    ax.legend()
    ax.grid(False)

    # Forth chart(shorts_count) -> 觀察是否放空
    ax = fig.add_subplot(614, sharex=ax)
    ax.plot(perf['shorts_count'], 
            label='shorts_count',
            linestyle='-',
            linewidth=3.0)
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))
    ax.axhline(y=0, c='r', linewidth=1)
    ax.legend()
    ax.grid(False)
    
    # Fifth chart(longs_count)
    ax = fig.add_subplot(615, sharex=ax)
    ax.plot(perf['longs_count'], 
            label='longs_count',
            linestyle='-',
            linewidth=3.0)
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))
    ax.axhline(y=0, c='r', linewidth=1)
    ax.legend()
    ax.grid(False) 
    
    # Sixth chart(weights) -> 觀察每日持股權重
    ax = fig.add_subplot(616, sharex=ax)        
    weights = pd.concat([df.to_frame(d) for d, df in perf['daily_weights'].dropna().items()],axis=1).T
    
    for i in weights.columns:
        df = weights.loc[:,i]
        ax.scatter(df.index,df.values,marker='.', s=5, c='grey', label='daily_weights')
    ax.axhline(y=0, c='r', linewidth=1)
    ax.legend(['daily_weights'])
    ax.grid(False)

    fig.tight_layout()

def record_vars(context, data):
    """
    Plot variables at the end of each day.
    """
            
    record(daily_weights=context.daily_weights,
           Market_Cap_Dollars=context.output.Market_Cap_Dollars
          )
```


```python
algo = TargetPercentPipeAlgo(
                     start_session=algo_start_dt,
                     end_session=end_dt,
                     limit_buy_multiplier=1.015,
                     allow_short=True,
                     custom_weight=True,
                     cancel_datedelta=2,
                     pipeline=make_pipeline,
                     analyze=analyze,
                     record_vars=record_vars,
                     get_record_vars=True,
                     get_transaction_detail=True
)

# set_algo_instance
set_algo_instance(algo)

# run
stats = algo.run()
```

    [2024-03-13 02:41:07.483598]: INFO: earn_dividends: Equity(6 [1590]), cash_dividend amount: 13.43905496, pay_date: 2023-10-30, div_owed: 48703.13517504
    [2024-03-13 02:41:07.483598]: INFO: handle_split: after split: asset: Equity(6 [1590]), amount: 3622, cost_basis: 982.73, last_sale_price: 981.0
    [2024-03-13 02:41:07.483598]: INFO: handle_split: returning cash: 787.02
    [2024-03-13 02:41:07.583608]: INFO: exec_cancel: Cancel_order: current time: 2023-10-03,
                                  due to : created>=current time + cancel_datedelta(2 days),
                                  created: 2023-09-28, asset: Equity(6 [1590]), amount: 2145, filled: 0'
    [2024-03-13 02:41:07.600275]: INFO: handle_simulation_end: Simulated 8 trading days
    first open: 2023-09-21 01:01:00+00:00
    last close: 2023-10-03 05:30:00+00:00
    


    
![png](output_92_1.png)
    



```python
result = run_pipeline(make_pipeline(), algo_start, end)
result.query('(longs == True) | (shorts == True)' )
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th></th>
      <th>Market_Cap_Dollars</th>
      <th>longs</th>
      <th>shorts</th>
      <th>long_weights</th>
      <th>short_weights</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="4" valign="top">2023-09-22 00:00:00+00:00</th>
      <th>Equity(6 [1590])</th>
      <td>1.980000e+11</td>
      <td>True</td>
      <td>False</td>
      <td>0.447512</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>Equity(10 [2301])</th>
      <td>2.818339e+11</td>
      <td>False</td>
      <td>True</td>
      <td>NaN</td>
      <td>0.507101</td>
    </tr>
    <tr>
      <th>Equity(24 [2603])</th>
      <td>2.444465e+11</td>
      <td>True</td>
      <td>False</td>
      <td>0.552488</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>Equity(39 [2912])</th>
      <td>2.739405e+11</td>
      <td>False</td>
      <td>True</td>
      <td>NaN</td>
      <td>0.492899</td>
    </tr>
    <tr>
      <th rowspan="4" valign="top">2023-09-25 00:00:00+00:00</th>
      <th>Equity(10 [2301])</th>
      <td>2.842023e+11</td>
      <td>False</td>
      <td>True</td>
      <td>NaN</td>
      <td>0.590585</td>
    </tr>
    <tr>
      <th>Equity(25 [2609])</th>
      <td>1.599384e+11</td>
      <td>True</td>
      <td>False</td>
      <td>0.231433</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>Equity(31 [2883])</th>
      <td>1.970197e+11</td>
      <td>False</td>
      <td>True</td>
      <td>NaN</td>
      <td>0.409415</td>
    </tr>
    <tr>
      <th>Equity(34 [2886])</th>
      <td>5.311406e+11</td>
      <td>True</td>
      <td>False</td>
      <td>0.768567</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th rowspan="4" valign="top">2023-09-26 00:00:00+00:00</th>
      <th>Equity(8 [2002])</th>
      <td>4.122534e+11</td>
      <td>False</td>
      <td>True</td>
      <td>NaN</td>
      <td>0.591931</td>
    </tr>
    <tr>
      <th>Equity(10 [2301])</th>
      <td>2.842023e+11</td>
      <td>False</td>
      <td>True</td>
      <td>NaN</td>
      <td>0.408069</td>
    </tr>
    <tr>
      <th>Equity(33 [2885])</th>
      <td>3.197649e+11</td>
      <td>True</td>
      <td>False</td>
      <td>0.374557</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>Equity(34 [2886])</th>
      <td>5.339508e+11</td>
      <td>True</td>
      <td>False</td>
      <td>0.625443</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th rowspan="4" valign="top">2023-09-27 00:00:00+00:00</th>
      <th>Equity(10 [2301])</th>
      <td>2.842023e+11</td>
      <td>False</td>
      <td>True</td>
      <td>NaN</td>
      <td>0.516628</td>
    </tr>
    <tr>
      <th>Equity(17 [2357])</th>
      <td>2.659082e+11</td>
      <td>False</td>
      <td>True</td>
      <td>NaN</td>
      <td>0.483372</td>
    </tr>
    <tr>
      <th>Equity(25 [2609])</th>
      <td>1.604622e+11</td>
      <td>True</td>
      <td>False</td>
      <td>0.231780</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>Equity(34 [2886])</th>
      <td>5.318431e+11</td>
      <td>True</td>
      <td>False</td>
      <td>0.768220</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th rowspan="4" valign="top">2023-09-28 00:00:00+00:00</th>
      <th>Equity(6 [1590])</th>
      <td>1.982000e+11</td>
      <td>True</td>
      <td>False</td>
      <td>0.272277</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>Equity(10 [2301])</th>
      <td>2.842023e+11</td>
      <td>False</td>
      <td>True</td>
      <td>NaN</td>
      <td>0.509667</td>
    </tr>
    <tr>
      <th>Equity(34 [2886])</th>
      <td>5.297354e+11</td>
      <td>True</td>
      <td>False</td>
      <td>0.727723</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>Equity(39 [2912])</th>
      <td>2.734207e+11</td>
      <td>False</td>
      <td>True</td>
      <td>NaN</td>
      <td>0.490333</td>
    </tr>
    <tr>
      <th rowspan="4" valign="top">2023-10-02 00:00:00+00:00</th>
      <th>Equity(10 [2301])</th>
      <td>2.877548e+11</td>
      <td>False</td>
      <td>True</td>
      <td>NaN</td>
      <td>0.513247</td>
    </tr>
    <tr>
      <th>Equity(24 [2603])</th>
      <td>2.455047e+11</td>
      <td>True</td>
      <td>False</td>
      <td>0.316682</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>Equity(34 [2886])</th>
      <td>5.297354e+11</td>
      <td>True</td>
      <td>False</td>
      <td>0.683318</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>Equity(39 [2912])</th>
      <td>2.729008e+11</td>
      <td>False</td>
      <td>True</td>
      <td>NaN</td>
      <td>0.486753</td>
    </tr>
    <tr>
      <th rowspan="4" valign="top">2023-10-03 00:00:00+00:00</th>
      <th>Equity(2 [1301])</th>
      <td>5.067130e+11</td>
      <td>False</td>
      <td>True</td>
      <td>NaN</td>
      <td>0.437013</td>
    </tr>
    <tr>
      <th>Equity(6 [1590])</th>
      <td>2.010000e+11</td>
      <td>True</td>
      <td>False</td>
      <td>0.471237</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>Equity(14 [2327])</th>
      <td>2.255366e+11</td>
      <td>True</td>
      <td>False</td>
      <td>0.528763</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>Equity(30 [2882])</th>
      <td>6.527799e+11</td>
      <td>False</td>
      <td>True</td>
      <td>NaN</td>
      <td>0.562987</td>
    </tr>
  </tbody>
</table>
</div>



#### algo.positions


```python
# 計算實際股票部位的weight = 個股持股市值／所有股票部位的持股市值加總
pos = algo.positions

pos['mv'] = pos['amount'] * pos['last_sale_price']

positive_sum = pos[pos['mv'] > 0].groupby(level=0)['mv'].sum()
negative_sum = - pos[pos['mv'] < 0].groupby(level=0)['mv'].sum()
pos['sum'] = np.where(pos['mv'] > 0,
                            pos.index.map(positive_sum),
                            pos.index.map(negative_sum))

pos['weight'] = pos['mv'] / pos['sum']
pos
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>sid</th>
      <th>symbol</th>
      <th>asset</th>
      <th>amount</th>
      <th>cost_basis</th>
      <th>last_sale_price</th>
      <th>mv</th>
      <th>sum</th>
      <th>weight</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2023-09-22 13:30:00+08:00</th>
      <td>34</td>
      <td>2886</td>
      <td>Equity(34 [2886])</td>
      <td>147424</td>
      <td>37.854390</td>
      <td>37.80</td>
      <td>5572627.20</td>
      <td>8058923.60</td>
      <td>0.691485</td>
    </tr>
    <tr>
      <th>2023-09-22 13:30:00+08:00</th>
      <td>46</td>
      <td>4904</td>
      <td>Equity(46 [4904])</td>
      <td>34484</td>
      <td>72.204762</td>
      <td>72.10</td>
      <td>2486296.40</td>
      <td>8058923.60</td>
      <td>0.308515</td>
    </tr>
    <tr>
      <th>2023-09-22 13:30:00+08:00</th>
      <td>10</td>
      <td>2301</td>
      <td>Equity(10 [2301])</td>
      <td>-33495</td>
      <td>119.468922</td>
      <td>120.00</td>
      <td>-4019400.00</td>
      <td>8113352.50</td>
      <td>-0.495406</td>
    </tr>
    <tr>
      <th>2023-09-22 13:30:00+08:00</th>
      <td>44</td>
      <td>3231</td>
      <td>Equity(44 [3231])</td>
      <td>-39941</td>
      <td>102.046428</td>
      <td>102.50</td>
      <td>-4093952.50</td>
      <td>8113352.50</td>
      <td>-0.504594</td>
    </tr>
    <tr>
      <th>2023-09-25 13:30:00+08:00</th>
      <td>10</td>
      <td>2301</td>
      <td>Equity(10 [2301])</td>
      <td>-33646</td>
      <td>119.468898</td>
      <td>120.00</td>
      <td>-4037520.00</td>
      <td>7969312.50</td>
      <td>-0.506633</td>
    </tr>
    <tr>
      <th>2023-09-25 13:30:00+08:00</th>
      <td>24</td>
      <td>2603</td>
      <td>Equity(24 [2603])</td>
      <td>38418</td>
      <td>116.165508</td>
      <td>116.00</td>
      <td>4456488.00</td>
      <td>8011632.00</td>
      <td>0.556252</td>
    </tr>
    <tr>
      <th>2023-09-25 13:30:00+08:00</th>
      <td>6</td>
      <td>1590</td>
      <td>Equity(6 [1590])</td>
      <td>3624</td>
      <td>982.405312</td>
      <td>981.00</td>
      <td>3555144.00</td>
      <td>8011632.00</td>
      <td>0.443748</td>
    </tr>
    <tr>
      <th>2023-09-25 13:30:00+08:00</th>
      <td>39</td>
      <td>2912</td>
      <td>Equity(39 [2912])</td>
      <td>-14865</td>
      <td>263.316060</td>
      <td>264.50</td>
      <td>-3931792.50</td>
      <td>7969312.50</td>
      <td>-0.493367</td>
    </tr>
    <tr>
      <th>2023-09-26 13:30:00+08:00</th>
      <td>10</td>
      <td>2301</td>
      <td>Equity(10 [2301])</td>
      <td>-39051</td>
      <td>119.468888</td>
      <td>120.00</td>
      <td>-4686120.00</td>
      <td>7921019.20</td>
      <td>-0.591606</td>
    </tr>
    <tr>
      <th>2023-09-26 13:30:00+08:00</th>
      <td>25</td>
      <td>2609</td>
      <td>Equity(25 [2609])</td>
      <td>39964</td>
      <td>46.015565</td>
      <td>45.95</td>
      <td>1836345.80</td>
      <td>7910665.20</td>
      <td>0.232135</td>
    </tr>
    <tr>
      <th>2023-09-26 13:30:00+08:00</th>
      <td>34</td>
      <td>2886</td>
      <td>Equity(34 [2886])</td>
      <td>160484</td>
      <td>37.904710</td>
      <td>37.85</td>
      <td>6074319.40</td>
      <td>7910665.20</td>
      <td>0.767865</td>
    </tr>
    <tr>
      <th>2023-09-26 13:30:00+08:00</th>
      <td>31</td>
      <td>2883</td>
      <td>Equity(31 [2883])</td>
      <td>-274144</td>
      <td>11.747586</td>
      <td>11.80</td>
      <td>-3234899.20</td>
      <td>7921019.20</td>
      <td>-0.408394</td>
    </tr>
    <tr>
      <th>2023-09-27 13:30:00+08:00</th>
      <td>10</td>
      <td>2301</td>
      <td>Equity(10 [2301])</td>
      <td>-26553</td>
      <td>119.388370</td>
      <td>120.00</td>
      <td>-3186360.00</td>
      <td>7701800.80</td>
      <td>-0.413716</td>
    </tr>
    <tr>
      <th>2023-09-27 13:30:00+08:00</th>
      <td>34</td>
      <td>2886</td>
      <td>Equity(34 [2886])</td>
      <td>129025</td>
      <td>37.945400</td>
      <td>37.70</td>
      <td>4864242.50</td>
      <td>7806390.10</td>
      <td>0.623110</td>
    </tr>
    <tr>
      <th>2023-09-27 13:30:00+08:00</th>
      <td>33</td>
      <td>2885</td>
      <td>Equity(33 [2885])</td>
      <td>116984</td>
      <td>25.185992</td>
      <td>25.15</td>
      <td>2942147.60</td>
      <td>7806390.10</td>
      <td>0.376890</td>
    </tr>
    <tr>
      <th>2023-09-27 13:30:00+08:00</th>
      <td>8</td>
      <td>2002</td>
      <td>Equity(8 [2002])</td>
      <td>-177424</td>
      <td>25.337349</td>
      <td>25.45</td>
      <td>-4515440.80</td>
      <td>7701800.80</td>
      <td>-0.586284</td>
    </tr>
    <tr>
      <th>2023-09-28 13:30:00+08:00</th>
      <td>10</td>
      <td>2301</td>
      <td>Equity(10 [2301])</td>
      <td>-33420</td>
      <td>119.711756</td>
      <td>121.50</td>
      <td>-4060530.00</td>
      <td>7843566.00</td>
      <td>-0.517689</td>
    </tr>
    <tr>
      <th>2023-09-28 13:30:00+08:00</th>
      <td>34</td>
      <td>2886</td>
      <td>Equity(34 [2886])</td>
      <td>158184</td>
      <td>37.910076</td>
      <td>37.70</td>
      <td>5963536.80</td>
      <td>7745131.35</td>
      <td>0.769972</td>
    </tr>
    <tr>
      <th>2023-09-28 13:30:00+08:00</th>
      <td>25</td>
      <td>2609</td>
      <td>Equity(25 [2609])</td>
      <td>39199</td>
      <td>45.514840</td>
      <td>45.45</td>
      <td>1781594.55</td>
      <td>7745131.35</td>
      <td>0.230028</td>
    </tr>
    <tr>
      <th>2023-09-28 13:30:00+08:00</th>
      <td>17</td>
      <td>2357</td>
      <td>Equity(17 [2357])</td>
      <td>-10308</td>
      <td>365.375480</td>
      <td>367.00</td>
      <td>-3783036.00</td>
      <td>7843566.00</td>
      <td>-0.482311</td>
    </tr>
    <tr>
      <th>2023-10-02 13:30:00+08:00</th>
      <td>10</td>
      <td>2301</td>
      <td>Equity(10 [2301])</td>
      <td>-33420</td>
      <td>119.711756</td>
      <td>125.50</td>
      <td>-4194210.00</td>
      <td>6449435.00</td>
      <td>-0.650322</td>
    </tr>
    <tr>
      <th>2023-10-02 13:30:00+08:00</th>
      <td>34</td>
      <td>2886</td>
      <td>Equity(34 [2886])</td>
      <td>148920</td>
      <td>37.920431</td>
      <td>37.60</td>
      <td>5599392.00</td>
      <td>5599392.00</td>
      <td>1.000000</td>
    </tr>
    <tr>
      <th>2023-10-02 13:30:00+08:00</th>
      <td>39</td>
      <td>2912</td>
      <td>Equity(39 [2912])</td>
      <td>-8575</td>
      <td>261.819714</td>
      <td>263.00</td>
      <td>-2255225.00</td>
      <td>6449435.00</td>
      <td>-0.349678</td>
    </tr>
    <tr>
      <th>2023-10-03 13:30:00+08:00</th>
      <td>10</td>
      <td>2301</td>
      <td>Equity(10 [2301])</td>
      <td>-32363</td>
      <td>119.706008</td>
      <td>123.00</td>
      <td>-3980649.00</td>
      <td>7756069.00</td>
      <td>-0.513230</td>
    </tr>
    <tr>
      <th>2023-10-03 13:30:00+08:00</th>
      <td>34</td>
      <td>2886</td>
      <td>Equity(34 [2886])</td>
      <td>136276</td>
      <td>37.935899</td>
      <td>37.65</td>
      <td>5130791.40</td>
      <td>7442672.40</td>
      <td>0.689375</td>
    </tr>
    <tr>
      <th>2023-10-03 13:30:00+08:00</th>
      <td>39</td>
      <td>2912</td>
      <td>Equity(39 [2912])</td>
      <td>-14410</td>
      <td>261.422628</td>
      <td>262.00</td>
      <td>-3775420.00</td>
      <td>7756069.00</td>
      <td>-0.486770</td>
    </tr>
    <tr>
      <th>2023-10-03 13:30:00+08:00</th>
      <td>24</td>
      <td>2603</td>
      <td>Equity(24 [2603])</td>
      <td>20922</td>
      <td>110.657508</td>
      <td>110.50</td>
      <td>2311881.00</td>
      <td>7442672.40</td>
      <td>0.310625</td>
    </tr>
  </tbody>
</table>
</div>



#### algo.dict_record_vars


```python
record_vars = algo.dict_record_vars
```


```python
# 實際持股市值 = 個股持股市值／所有股票部位的持股市值加總 * max_leverage
record_vars['daily_weights']
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>date</th>
      <th>symbol</th>
      <th>daily_weights</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2023-09-22 00:00:00+08:00</td>
      <td>2886</td>
      <td>0.559924</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2023-09-22 00:00:00+08:00</td>
      <td>4904</td>
      <td>0.249817</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2023-09-22 00:00:00+08:00</td>
      <td>2301</td>
      <td>-0.403860</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2023-09-22 00:00:00+08:00</td>
      <td>3231</td>
      <td>-0.411351</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2023-09-25 00:00:00+08:00</td>
      <td>2301</td>
      <td>-0.407072</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2023-09-25 00:00:00+08:00</td>
      <td>2603</td>
      <td>0.449313</td>
    </tr>
    <tr>
      <th>6</th>
      <td>2023-09-25 00:00:00+08:00</td>
      <td>1590</td>
      <td>0.358437</td>
    </tr>
    <tr>
      <th>7</th>
      <td>2023-09-25 00:00:00+08:00</td>
      <td>2912</td>
      <td>-0.396412</td>
    </tr>
    <tr>
      <th>8</th>
      <td>2023-09-26 00:00:00+08:00</td>
      <td>2886</td>
      <td>0.622353</td>
    </tr>
    <tr>
      <th>9</th>
      <td>2023-09-26 00:00:00+08:00</td>
      <td>2301</td>
      <td>-0.480123</td>
    </tr>
    <tr>
      <th>10</th>
      <td>2023-09-26 00:00:00+08:00</td>
      <td>2609</td>
      <td>0.188145</td>
    </tr>
    <tr>
      <th>11</th>
      <td>2023-09-26 00:00:00+08:00</td>
      <td>2883</td>
      <td>-0.331436</td>
    </tr>
    <tr>
      <th>12</th>
      <td>2023-09-27 00:00:00+08:00</td>
      <td>2886</td>
      <td>0.501285</td>
    </tr>
    <tr>
      <th>13</th>
      <td>2023-09-27 00:00:00+08:00</td>
      <td>2301</td>
      <td>-0.328371</td>
    </tr>
    <tr>
      <th>14</th>
      <td>2023-09-27 00:00:00+08:00</td>
      <td>2885</td>
      <td>0.303203</td>
    </tr>
    <tr>
      <th>15</th>
      <td>2023-09-27 00:00:00+08:00</td>
      <td>2002</td>
      <td>-0.465339</td>
    </tr>
    <tr>
      <th>16</th>
      <td>2023-09-28 00:00:00+08:00</td>
      <td>2886</td>
      <td>0.618396</td>
    </tr>
    <tr>
      <th>17</th>
      <td>2023-09-28 00:00:00+08:00</td>
      <td>2301</td>
      <td>-0.421061</td>
    </tr>
    <tr>
      <th>18</th>
      <td>2023-09-28 00:00:00+08:00</td>
      <td>2609</td>
      <td>0.184744</td>
    </tr>
    <tr>
      <th>19</th>
      <td>2023-09-28 00:00:00+08:00</td>
      <td>2357</td>
      <td>-0.392286</td>
    </tr>
    <tr>
      <th>20</th>
      <td>2023-10-02 00:00:00+08:00</td>
      <td>2886</td>
      <td>0.597376</td>
    </tr>
    <tr>
      <th>21</th>
      <td>2023-10-02 00:00:00+08:00</td>
      <td>2301</td>
      <td>-0.447463</td>
    </tr>
    <tr>
      <th>22</th>
      <td>2023-10-02 00:00:00+08:00</td>
      <td>2912</td>
      <td>-0.240600</td>
    </tr>
    <tr>
      <th>23</th>
      <td>2023-10-03 00:00:00+08:00</td>
      <td>2886</td>
      <td>0.542337</td>
    </tr>
    <tr>
      <th>24</th>
      <td>2023-10-03 00:00:00+08:00</td>
      <td>2301</td>
      <td>-0.420764</td>
    </tr>
    <tr>
      <th>25</th>
      <td>2023-10-03 00:00:00+08:00</td>
      <td>2603</td>
      <td>0.244371</td>
    </tr>
    <tr>
      <th>26</th>
      <td>2023-10-03 00:00:00+08:00</td>
      <td>2912</td>
      <td>-0.399071</td>
    </tr>
  </tbody>
</table>
</div>




```python
# 個股總市值
record_vars['Market_Cap_Dollars']
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>date</th>
      <th>symbol</th>
      <th>Market_Cap_Dollars</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2023-09-21 00:00:00+08:00</td>
      <td>1101</td>
      <td>2.458148e+11</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2023-09-21 00:00:00+08:00</td>
      <td>1216</td>
      <td>4.028549e+11</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2023-09-21 00:00:00+08:00</td>
      <td>1301</td>
      <td>5.245370e+11</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2023-09-21 00:00:00+08:00</td>
      <td>1303</td>
      <td>5.424682e+11</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2023-09-21 00:00:00+08:00</td>
      <td>1326</td>
      <td>3.798049e+11</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>443</th>
      <td>2023-10-03 00:00:00+08:00</td>
      <td>6415</td>
      <td>1.188855e+11</td>
    </tr>
    <tr>
      <th>444</th>
      <td>2023-10-03 00:00:00+08:00</td>
      <td>6505</td>
      <td>7.649346e+11</td>
    </tr>
    <tr>
      <th>445</th>
      <td>2023-10-03 00:00:00+08:00</td>
      <td>6669</td>
      <td>2.867389e+11</td>
    </tr>
    <tr>
      <th>446</th>
      <td>2023-10-03 00:00:00+08:00</td>
      <td>9910</td>
      <td>1.920654e+11</td>
    </tr>
    <tr>
      <th>447</th>
      <td>2023-10-03 00:00:00+08:00</td>
      <td>IR0001</td>
      <td>5.189094e+13</td>
    </tr>
  </tbody>
</table>
<p>448 rows × 3 columns</p>
</div>



[Return to Menu](#menu) 
