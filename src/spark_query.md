# Query and results
This document only shows the spark queries and results.

## Load the entire dataset
Due to complexity, I'm not going to explain how to run spark and cassandra. Essentially, I had to create a spark shell environment that comes with pre-configured sparkSession named `spark`
```py
from pyspark.sql.functions import avg

df = spark.read\
    .format("org.apache.spark.sql.cassandra")\
    .options(table="rate", keyspace="cpts_415")\
    .load()
```
## Group by race
Show average `individualrate` `annualincome` `amountpaidmedical` group by `race`
```py
df.groupby('race')\
    .avg("individualrate", "annualincome", "amountpaidmedical")\
    .orderBy("avg(individualrate)").show()
```

Results
```
+--------------------+-------------------+------------------+----------------------+
|                race|avg(individualrate)| avg(annualincome)|avg(amountpaidmedical)|
+--------------------+-------------------+------------------+----------------------+
|          Black-Asia| 1673.0584302010138| 23286.05574439157|     553.8851121685927|
|            Black-AI|  2578.143319904254|25497.676536816034|     575.3767169556406|
| White-Black-AI-Asia| 3069.2982976197386| 26500.25661016949|    508.03966101694914|
|      White-Asian-HP| 3749.7039744424333|25368.026962457338|     602.9389078498293|
|         White-Black|  3970.549880985011|25428.854601441308|     639.0141672381002|
|              Black |   4036.06903262784|25525.133306958003|     616.7550198479561|
|              White |  4094.536279777124|25481.115625622784|     616.0977226974492|
|              Asian |  4125.682846785707| 25520.85031458689|     625.5864693657828|
|      White-Black-AI|   4169.96200675268| 25220.46178630301|     657.1309755732054|
|            White-AI|  4273.274012385423|25719.779632771784|     600.2097927523937|
|American Indian,A...|  4289.781773954732|25425.329499799354|      616.518466586408|
|Hawaiian/Pacific ...|  4359.008564200329|25293.570666609547|     593.7200968211807|
|            Asian-HP|  4401.668070723344|26574.597826086956|     690.2313179347826|
|          White-Asia|  4770.187848817139|25152.390307380418|     581.3867733274889|
|            White-HP|  5138.955811430343|25109.684499314128|     579.0352080475537|
+--------------------+-------------------+------------------+----------------------+
```

## Group by state
```py
df.groupby('stateCode')\
    .avg("individualrate", "annualincome", "amountpaidmedical")\
    .orderBy("avg(individualrate)").show(60) # default only shows 20 rows
```

Results
```
+---------+-------------------+------------------+----------------------+       
|stateCode|avg(individualrate)| avg(annualincome)|avg(amountpaidmedical)|
+---------+-------------------+------------------+----------------------+
|       KY| 32.554621848739494|21315.831932773108|    309.98319327731093|
|       CA| 47.189542483660134| 25891.21699346405|     762.6026143790849|
|       RI| 58.266666666666666|31466.666666666668|     322.1066666666667|
|       MN|  65.47826086956522|30991.165217391303|                 896.8|
|       VT|  68.53398058252426|29981.058252427185|    289.86407766990294|
|       WA|  69.18666666666667|33231.346666666665|     514.1466666666666|
|       CT|  71.65882352941176|31943.211764705884|                 440.2|
|       MA|  87.98224852071006|33405.136094674555|    475.72189349112426|
|       NY|  87.98843930635839| 27741.41040462428|     553.2254335260116|
|       CO|              93.33|          36599.22|                823.73|
|       MD| 112.17037037037036| 33786.77777777778|    409.68888888888887|
|       DC|        188.2265625|      51898.578125|            836.953125|
|       HI|  256.3414756846522|27813.455204216072|     641.4552042160738|
|       OR|  276.7092536648348| 25690.85714911065|     631.2063239607196|
|       NV| 298.63338941324537|25672.273738317755|     600.5572274143302|
|       FL|  311.1537705634762|25485.383907981177|     616.7154431726692|
|       VA|  319.2546529723141|  25568.4589524266|     607.9995268468335|
|       IA|  319.4996305203499| 25642.98137766609|     612.8296340556151|
|       WV| 347.54515820796627| 25493.31990869771|     616.7218479484671|
|       NH| 350.08564661123336|25457.114587321557|      620.723513646557|
|       ME|  355.3263623444313|25192.895734336802|     616.2497047271129|
|       DE|  357.5608867316222|26082.624016976657|     619.7295593558856|
|       IL| 372.29507781029275|25643.924199879923|     618.8002008959497|
|       NJ|  421.7653685229286| 25381.30949070201|     610.7546980388537|
|       WI| 476.98608721016757|  25489.5733940654|     618.1287530355344|
|       MI| 3052.0369085360885|25518.673646132484|        612.8111098191|
|       OH|      3742.80970777|25432.924338736768|     617.0382934763197|
|       PA| 3743.4803593717024|25548.163359536546|     615.9930940475865|
|       AZ| 3975.0602803789016|25580.634392081378|     614.9670209788612|
|       IN|   4854.63933713525|25454.280395941994|     621.2084628394832|
|       MT|  5128.751770112831|25583.792514080626|     615.8962940061565|
|       OK|  5154.282819981159|25364.312492965975|     624.6063517924038|
|       SC|  5680.842843673561|25524.613868198212|     614.9398820519608|
|       TX|  5706.276712015512| 25409.17274800854|     618.0021704961333|
|       GA|  6424.645148601987|25465.582609800516|     621.6753229426962|
|       UT|  6700.177326899099| 25608.56033061716|     634.7455253282205|
|       AK|  7015.867723251083| 25290.98719859918|     615.4253803743404|
|       ND|  7209.805689039068|25241.808467491224|     607.0317865717757|
|       NC|    8202.3436106871|25446.407674962076|     618.3710138985691|
|       LA|   8706.35248943729|25304.446937946246|      618.631787310617|
|       NE|  9141.814151222097|25456.901068792733|     641.0891318835929|
|       TN|  9598.181202037998|25447.714495203698|     614.1774375328184|
|       SD|   9821.94066087135|25399.756283090504|     622.3529960455614|
|       KS| 11337.078981654118|25606.106336156052|     608.2010087850566|
|       WY| 12068.475379951475|25057.843934383713|      583.112321424712|
|       NM| 12098.057370134567|25691.582901213445|     601.2564291542734|
|       AL| 12120.783486616576|25395.486738551313|     611.7416364328301|
|       MO| 12294.417444199054|25387.190480662168|     608.6901385777337|
|       MS| 12923.767440503434|25533.642388297623|     610.1819353931268|
|       AR| 15039.555409055327|25430.241790060973|     616.3323171449422|
|       ID| 18426.675087824242|25320.550403979414|     620.2144828775986|
+---------+-------------------+------------------+----------------------+
```

## amountpaid/income for each state
```py
df.withColumn("paidratio", df["amountpaidmedical"] / df["annualincome"])\
    .orderBy("paidratio").show(60)
```

Result:
It doesn't work because a lot of rows have 0 for either `amountpaidmedical` or `annualincome`

## Group by sex
```py
df.groupby('sex')\
    .avg("individualrate", "annualincome", "amountpaidmedical")\
    .orderBy("avg(individualrate)").show()
```

Result
```
+---+-------------------+------------------+----------------------+             
|sex|avg(individualrate)| avg(annualincome)|avg(amountpaidmedical)|
+---+-------------------+------------------+----------------------+
|  2| 4091.9120450776904|25503.772673696145|     615.4658294076827|
|  1| 4098.9003353278895|25466.407295984267|     617.5335003485052|
+---+-------------------+------------------+----------------------+
```

## Group by age
```py
df.groupby('age')\
    .avg("individualrate", "annualincome", "amountpaidmedical")\
    .orderBy("avg(individualrate)").show(100)
```

Result
```
+-------------+-------------------+------------------+----------------------+   
|          age|avg(individualrate)| avg(annualincome)|avg(amountpaidmedical)|
+-------------+-------------------+------------------+----------------------+
|            4|                0.0|               0.0|             405.15625|
|           12|                0.0|               0.0|     191.5748031496063|
|            3|                0.0|               0.0|    164.17708333333334|
|            5|                0.0|               0.0|     195.4950495049505|
|           13|                0.0|               0.0|    195.18018018018017|
|           14|                0.0|               0.0|    240.80357142857142|
|            6|                0.0|               0.0|    161.93548387096774|
|            2|                0.0|               0.0|    199.54736842105262|
|            9|                0.0|               0.0|    152.69565217391303|
|           11|                0.0|               0.0|    235.04132231404958|
|            8|                0.0|               0.0|    248.05309734513276|
|            1|                0.0|               0.0|     259.7008547008547|
|            7|                0.0|               0.0|     169.9579831932773|
|           10|                0.0|               0.0|     167.3587786259542|
|            0|                0.0|               0.0|    409.63291139240505|
|           15|           1.296875|       1239.296875|            233.515625|
|           16| 1.5809523809523809| 1572.009523809524|    275.93333333333334|
|           17| 1.9130434782608696| 2814.330434782609|    204.11304347826086|
|           19| 3.3645833333333335| 7006.260416666667|             234.71875|
|           18|  8.813725490196079| 3336.705882352941|    192.13725490196077|
|           20| 21.838709677419356|14627.118279569893|     269.6236559139785|
|Family Option| 28.528599661523227|25198.509171734935|      622.851884419042|
|           73|  67.15492957746478|21346.985915492958|     772.0281690140845|
|           79|              73.55|            4000.0|               447.925|
|           71|  83.44705882352942| 15135.29411764706|     721.0235294117647|
|           85|  88.86390532544378|            2000.0|     687.7573964497042|
|           70|  92.13131313131314|15442.858585858587|     776.2727272727273|
|           74| 103.91463414634147| 8104.219512195122|     1191.719512195122|
|           66| 104.58333333333333|19998.380952380954|     623.5238095238095|
|           65| 108.46610169491525|25829.177966101695|     596.9406779661017|
|           67| 108.58241758241758| 19669.53846153846|    1306.6483516483515|
|           69| 110.97938144329896|18910.907216494845|    1032.5567010309278|
|           76| 114.11320754716981| 5716.981132075472|     931.0377358490566|
|           68| 115.43157894736842|16373.894736842105|     931.0842105263158|
|         0-20|  122.2559431704394| 25614.55150296382|     610.0384262166548|
|           77| 123.34285714285714| 5166.557142857143|    478.15714285714284|
|           75| 131.97014925373134|14517.910447761195|     614.3283582089553|
|           72| 138.10666666666665|12702.133333333333|     812.0666666666667|
|           80|  205.1129943502825| 4836.576271186441|     801.7005649717514|
|           78|  211.4090909090909|2909.7954545454545|     548.7954545454545|
|           21|  4081.338092990465|25533.908270917145|     625.3839967438192|
|           22| 4081.4440816131464|25375.823382976094|     607.6240741952131|
|           24|  4081.451807054158|25572.012940714736|     610.2013969139975|
|           23|  4081.578543328337|25477.375064961496|     615.5362603256979|
|           25|  4082.219289206199|25487.781075217234|     608.9740481096659|
|           26| 4085.4687812487036|25518.418279538633|     617.8269508906833|
|           27|  4089.255307299647| 25502.03217941404|     622.2541577794169|
|           28|  4096.318270640761|25340.019020208518|     620.5306507353342|
|           29|  4101.728285005097|25619.991108026396|     613.3822567516497|
|           30|  4104.821514254547|25527.099308795954|     626.8978457110462|
|           31|  4108.996190946069| 25397.81408423065|     627.8893605113874|
|           32|  4112.924569416915|25497.013884952215|      615.605810530906|
|           33|  4115.877156846646| 25291.60650220956|     621.5636774624957|
|           34|  4118.649046798707| 25397.10300215504|     614.6204223586233|
|           35|  4119.906337471401|25425.700825629396|     622.3118313564742|
|           36|  4121.335330956664| 25486.87921855638|     617.5354400526195|
|           37| 4123.0421503770285| 25648.97606873017|     625.6618658758626|
|           38|  4124.240407365019| 25402.63260411367|      610.137281052402|
|           39| 4126.8739162772545|25565.352335823714|      613.625250734033|
|           40|  4129.863276876284|25627.008067358813|     617.1380572201044|
|           41| 4134.1923739184185|25594.858729899155|     612.6650531479967|
|           42|  4138.309571966079|25404.376147922565|     624.8339686958292|
|           43|  4144.170697427902|25438.115186404706|     601.3502729380638|
|           44|  4151.201882714048|25706.089688608477|     613.3043372390533|
|           45|  4159.234975835349|25591.619209593893|     617.8067557009176|
|           46|  4169.170935841221|25638.030814126745|     611.5610431521336|
|           47|  4180.037181082741| 25489.46283805883|     615.1798571085931|
|           48|  4192.724905034372|25269.041469663658|     623.4379772136718|
|           49|  4204.818204612431|25500.665656433514|     610.4432343450201|
|           50|  4219.124356198273|25614.515472647123|     627.6437384386709|
|           51|  4232.801148124165|25340.564329557474|     615.5190846352605|
|           52|  4247.608565457612|25445.007027820575|     615.0693661152941|
|           53|  4263.086687413013|25416.090049494516|     611.8919115349114|
|           54|  4279.643820980553| 25449.87156043317|     613.5885892870122|
|           55|  4296.644348155056|25461.615670597643|     627.4014027946869|
|           56|  4314.430813816559|25407.709720793828|     617.5034578247139|
|           57| 4332.3248496155975| 25489.45849549579|     612.8001359075829|
|           58|  4351.846386682149| 25568.19268605568|     612.7893179926809|
|           59|  4361.339435966551|25501.576729363198|     628.2970587380264|
|           60| 4380.2341201465015|25476.915362756998|     620.8465831873779|
|           61|  4396.317099878345|25630.661340018894|     614.3064820870576|
|           62|  4407.753140359319|25446.777644532143|     613.4999509413803|
|           63|  4421.155962599299| 25534.69855153568|     608.5902110557147|
|           64|   4429.74473461214|25463.924140337105|     614.3648851965722|
|  65 and over|  4431.414199030391| 25470.39224988821|     609.2431625749363|
+-------------+-------------------+------------------+----------------------+
```

### still group by age, but order by amountpaidmedical
```py
df.groupby('age')\
    .avg("individualrate", "annualincome", "amountpaidmedical")\
    .orderBy("avg(amountpaidmedical)").show(100)
```

Result
```
+-------------+-------------------+------------------+----------------------+   
|          age|avg(individualrate)| avg(annualincome)|avg(amountpaidmedical)|
+-------------+-------------------+------------------+----------------------+
|            9|                0.0|               0.0|    152.69565217391303|
|            6|                0.0|               0.0|    161.93548387096774|
|            3|                0.0|               0.0|    164.17708333333334|
|           10|                0.0|               0.0|     167.3587786259542|
|            7|                0.0|               0.0|     169.9579831932773|
|           12|                0.0|               0.0|     191.5748031496063|
|           18|  8.813725490196079| 3336.705882352941|    192.13725490196077|
|           13|                0.0|               0.0|    195.18018018018017|
|            5|                0.0|               0.0|     195.4950495049505|
|            2|                0.0|               0.0|    199.54736842105262|
|           17| 1.9130434782608696| 2814.330434782609|    204.11304347826086|
|           15|           1.296875|       1239.296875|            233.515625|
|           19| 3.3645833333333335| 7006.260416666667|             234.71875|
|           11|                0.0|               0.0|    235.04132231404958|
|           14|                0.0|               0.0|    240.80357142857142|
|            8|                0.0|               0.0|    248.05309734513276|
|            1|                0.0|               0.0|     259.7008547008547|
|           20| 21.838709677419356|14627.118279569893|     269.6236559139785|
|           16| 1.5809523809523809| 1572.009523809524|    275.93333333333334|
|            4|                0.0|               0.0|             405.15625|
|            0|                0.0|               0.0|    409.63291139240505|
|           79|              73.55|            4000.0|               447.925|
|           77| 123.34285714285714| 5166.557142857143|    478.15714285714284|
|           78|  211.4090909090909|2909.7954545454545|     548.7954545454545|
|           65| 108.46610169491525|25829.177966101695|     596.9406779661017|
|           43|  4144.170697427902|25438.115186404706|     601.3502729380638|
|           22| 4081.4440816131464|25375.823382976094|     607.6240741952131|
|           63|  4421.155962599299| 25534.69855153568|     608.5902110557147|
|           25|  4082.219289206199|25487.781075217234|     608.9740481096659|
|  65 and over|  4431.414199030391| 25470.39224988821|     609.2431625749363|
|         0-20|  122.2559431704394| 25614.55150296382|     610.0384262166548|
|           38|  4124.240407365019| 25402.63260411367|      610.137281052402|
|           24|  4081.451807054158|25572.012940714736|     610.2013969139975|
|           49|  4204.818204612431|25500.665656433514|     610.4432343450201|
|           46|  4169.170935841221|25638.030814126745|     611.5610431521336|
|           53|  4263.086687413013|25416.090049494516|     611.8919115349114|
|           41| 4134.1923739184185|25594.858729899155|     612.6650531479967|
|           58|  4351.846386682149| 25568.19268605568|     612.7893179926809|
|           57| 4332.3248496155975| 25489.45849549579|     612.8001359075829|
|           44|  4151.201882714048|25706.089688608477|     613.3043372390533|
|           29|  4101.728285005097|25619.991108026396|     613.3822567516497|
|           62|  4407.753140359319|25446.777644532143|     613.4999509413803|
|           54|  4279.643820980553| 25449.87156043317|     613.5885892870122|
|           39| 4126.8739162772545|25565.352335823714|      613.625250734033|
|           61|  4396.317099878345|25630.661340018894|     614.3064820870576|
|           75| 131.97014925373134|14517.910447761195|     614.3283582089553|
|           64|   4429.74473461214|25463.924140337105|     614.3648851965722|
|           34|  4118.649046798707| 25397.10300215504|     614.6204223586233|
|           52|  4247.608565457612|25445.007027820575|     615.0693661152941|
|           47|  4180.037181082741| 25489.46283805883|     615.1798571085931|
|           51|  4232.801148124165|25340.564329557474|     615.5190846352605|
|           23|  4081.578543328337|25477.375064961496|     615.5362603256979|
|           32|  4112.924569416915|25497.013884952215|      615.605810530906|
|           40|  4129.863276876284|25627.008067358813|     617.1380572201044|
|           56|  4314.430813816559|25407.709720793828|     617.5034578247139|
|           36|  4121.335330956664| 25486.87921855638|     617.5354400526195|
|           45|  4159.234975835349|25591.619209593893|     617.8067557009176|
|           26| 4085.4687812487036|25518.418279538633|     617.8269508906833|
|           28|  4096.318270640761|25340.019020208518|     620.5306507353342|
|           60| 4380.2341201465015|25476.915362756998|     620.8465831873779|
|           33|  4115.877156846646| 25291.60650220956|     621.5636774624957|
|           27|  4089.255307299647| 25502.03217941404|     622.2541577794169|
|           35|  4119.906337471401|25425.700825629396|     622.3118313564742|
|Family Option| 28.528599661523227|25198.509171734935|      622.851884419042|
|           48|  4192.724905034372|25269.041469663658|     623.4379772136718|
|           66| 104.58333333333333|19998.380952380954|     623.5238095238095|
|           42|  4138.309571966079|25404.376147922565|     624.8339686958292|
|           21|  4081.338092990465|25533.908270917145|     625.3839967438192|
|           37| 4123.0421503770285| 25648.97606873017|     625.6618658758626|
|           30|  4104.821514254547|25527.099308795954|     626.8978457110462|
|           55|  4296.644348155056|25461.615670597643|     627.4014027946869|
|           50|  4219.124356198273|25614.515472647123|     627.6437384386709|
|           31|  4108.996190946069| 25397.81408423065|     627.8893605113874|
|           59|  4361.339435966551|25501.576729363198|     628.2970587380264|
|           85|  88.86390532544378|            2000.0|     687.7573964497042|
|           71|  83.44705882352942| 15135.29411764706|     721.0235294117647|
|           73|  67.15492957746478|21346.985915492958|     772.0281690140845|
|           70|  92.13131313131314|15442.858585858587|     776.2727272727273|
|           80|  205.1129943502825| 4836.576271186441|     801.7005649717514|
|           72| 138.10666666666665|12702.133333333333|     812.0666666666667|
|           76| 114.11320754716981| 5716.981132075472|     931.0377358490566|
|           68| 115.43157894736842|16373.894736842105|     931.0842105263158|
|           69| 110.97938144329896|18910.907216494845|    1032.5567010309278|
|           74| 103.91463414634147| 8104.219512195122|     1191.719512195122|
|           67| 108.58241758241758| 19669.53846153846|    1306.6483516483515|
+-------------+-------------------+------------------+----------------------+
```