# Databricks notebook source
# MAGIC %md
# MAGIC ## IOT Validaton Testing

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
import os

# COMMAND ----------

def columnCheck(df, col):
  for i in range(0,len(df.columns)):
    if col == df.columns[i]:
      return True
    
def is_delta_table(dir):
  for subDir in os.walk(dir):  
    if subDir[0].find("_delta_log") > -1:
      delta_table = True
      return delta_table

def is_parquet_file(domain, subDomain, dataProduct, version):
  baseDir = '/dbfs/mnt/data/published/{0}/{1}/{2}/ver={3}/'.format(domain, subDomain, dataProduct,version)
  parquetInd = 0
  nonParquetInd = 0
  for subDir in next(os.walk(baseDir))[1]:
    fullDir = baseDir+subDir
    if subDir != '_delta_log':
      for file in os.listdir(fullDir):
        if (file.find('.parquet') > -1):
          parquetInd += 1
        else:
          nonParquetInd += 1
      return (parquetInd, nonParquetInd)
    
def is_expected_schema(mappingDoc_Schema, product_DF):
  mappingDoc_DF = spark.createDataFrame(spark.sparkContext.emptyRDD(), mappingDoc_Schema)
  if mappingDoc_DF.schema == product_DF.schema:
    return True
  else:
    return False

# COMMAND ----------

dataLoadSchema = ( StructType().add("device_id", StringType())
                                      .add("message_id", StringType())
                                      .add("message_type", StringType())
                                      .add("topic", StringType())
                                      .add("storeid", StringType())
                                      .add("em_id", StringType())
                                      .add("body", 
                                          (StructType()
                                         .add("type", StringType())
                                         #.add("time", StringType())
                                         .add("version", StringType())
                                         .add("eventTime", StringType())
                                         .add("eventTimeLocal", StringType())
                                         .add("body", StructType()
                                              .add("location", 
                                              (StructType().add("countryCode", StringType())
                                              .add("description", StringType())
                                              .add("language", StringType())
                                              .add("timeZone", StringType())
                                              .add("utcOffset", StringType())
                                             )
                                                  )
                                              .add("modules",
                                                   ArrayType(
                                                            StructType()
                                                            .add("id", StringType())
                                                            .add("name", StringType())
                                                            )
                                                  )
                                              .add("networkInterfaces",
                                                   ArrayType(
                                                             StructType()
                                                             .add("address", StringType())
                                                             .add("name", StringType())
                                                            )
                                                  )
                                              .add("software", StructType().add("version", StringType()))
                                              .add("stateful", StringType())
                                              .add("source", StringType())
                                              .add("severity", StringType())
                                              .add("number", StringType())
                                              .add("module", StringType())
                                              .add("index", StringType())
                                              .add("internalText", StringType())
                                              .add("Recipe", StructType()
                                                  .add("Category", StringType())
                                                  .add("ParamCoffee", StructType()
                                                       .add("NumBrewCycles", LongType())
                                                       .add("Name", StringType())
                                                       .add("IsqExtrTime", DoubleType())
                                                       .add("IsqPuckThickn", DoubleType())
                                                       .add("MaxExtrTime", LongType())
                                                       .add("MaxValidExtrTime", LongType())
                                                       .add("MinValidExtrTime", LongType())
                                                       .add("Module", StringType())
                                                       .add("PostPress", LongType())
                                                       .add("PreBrew", LongType())
                                                       .add("PressFactor", DoubleType())
                                                       .add("RelaxTime", LongType())
                                                       .add("TotalAmount", LongType())
                                                       .add("UseExpChamber", BooleanType())
                                                       .add("UseForISQ", BooleanType())
                                                       .add("Valid", BooleanType())
                                                       .add("Grinder1", StructType()
                                                           .add("Amount", LongType())
                                                           .add("Valid", BooleanType())
                                                           )
                                                       .add("Grinder2", StructType()
                                                           .add("Amount", LongType())
                                                           .add("Valid", BooleanType())
                                                           )
                                                       .add("Grinder3", StructType()
                                                           .add("Amount", LongType())
                                                           .add("Valid", BooleanType())
                                                           )
                                                      )
                                                    .add("ParamCoffeeGrind", StructType()
                                                         .add("Module", StringType())
                                                         .add("Valid", BooleanType())
                                                         .add("Grinder1", StructType()
                                                             .add("Amount", LongType())
                                                             .add("Valid", BooleanType())
                                                             )
                                                         .add("Grinder2", StructType()
                                                             .add("Amount", LongType())
                                                             .add("Valid", BooleanType())
                                                             )
                                                         .add("Grinder3", StructType()
                                                             .add("Amount", LongType())
                                                             .add("Valid", BooleanType())
                                                             )
                                                        )
                                                    .add("ParamSteam", StructType()
                                                        .add("AutoOff", BooleanType())
                                                        .add("Module", StringType())
                                                        .add("Temperature", DoubleType())
                                                        .add("Valid", BooleanType())
                                                        )
                                                  )
                                                 .add("id", StringType())
                                                 .add("name", StringType())
                                                 .add("Result", StructType()
                                                      .add("Success", BooleanType())
                                                      .add("ResultCoffee", StructType()
                                                           .add("BrewChamberIdx", LongType())
                                                           .add("ErrorNo", LongType())
                                                           .add("BrewCycle1", StructType()
                                                               .add("ExtrTime", DoubleType())
                                                               .add("PuckThicknAfterPr", DoubleType())
                                                               .add("PuckThicknAfterSq", DoubleType())
                                                               .add("Valid", BooleanType())
                                                               )
                                                           .add("Grinder1", StructType()
                                                                .add("Adjustment", LongType())
                                                                .add("CalibFactor", DoubleType())
                                                                .add("Duration", DoubleType())
                                                                .add("Rate", DoubleType())
                                                                .add("Valid", BooleanType())
                                                               )
                                                           .add("Grinder2", StructType()
                                                                .add("Adjustment", LongType())
                                                                .add("CalibFactor", DoubleType())
                                                                .add("Duration", DoubleType())
                                                                .add("Rate", DoubleType())
                                                                .add("Valid", BooleanType())
                                                               )
                                                           .add("Grinder3", StructType()
                                                                .add("Adjustment", LongType())
                                                                .add("CalibFactor", DoubleType())
                                                                .add("Duration", DoubleType())
                                                                .add("Rate", DoubleType())
                                                                .add("Valid", BooleanType())
                                                               )
                                                           .add("InletWaterCond", LongType())
                                                           .add("InletWaterTemp", DoubleType())
                                                           .add("Module", StringType())
                                                           .add("Name", StringType())
                                                           .add("NumBrewCycles", LongType())
                                                           .add("Success", BooleanType())
                                                           .add("Valid", BooleanType())
                                                           .add("WaterAmount", DoubleType())
                                                          )
                                                      .add("ResultCoffeeClean", StructType()
                                                           .add("ErrorNo", LongType())
                                                           .add("Module", StringType())
                                                           .add("Success", BooleanType())
                                                           .add("Valid", BooleanType())
                                                           .add("WaterAmount", LongType())
                                                          )
                                                      .add("ResultCoffeeRinse1", StructType()
                                                           .add("ErrorNo", LongType())
                                                           .add("Name", StringType())
                                                           .add("Success", BooleanType())
                                                           .add("Valid", BooleanType())
                                                           .add("WaterAmount", DoubleType())
                                                          )
                                                      .add("ResultCoffeeRinse2", StructType()
                                                           .add("ErrorNo", LongType())
                                                           .add("Name", StringType())
                                                           .add("Success", BooleanType())
                                                           .add("Valid", BooleanType())
                                                           .add("WaterAmount", DoubleType())
                                                          )
                                                      .add("ResultSteam", StructType()
                                                           .add("AutoStop", BooleanType())
                                                           .add("Duration", LongType())
                                                           .add("EndTemp", DoubleType())
                                                           .add("ErrorNo", LongType())
                                                           .add("ManualStop", BooleanType())
                                                           .add("Module", StringType())
                                                           .add("Success", BooleanType())
                                                           .add("Valid", BooleanType())
                                                          )
                                                     )
                                             
                                              )
                                          )
                                          )
                        )

# COMMAND ----------

partFiles = '/mnt/data/raw/Store/StoreEquipment/EspressoMachineTelemetry/ver=01/2019/09/09/*/*/*.json'

rawPartDF = (spark.read
             .json(partFiles, schema=dataLoadSchema)
#              .schema(dataLoadSchema)
#              .json(partFiles)
            )
display(rawPartDF)

# COMMAND ----------

inc_machine_prepared_df = (rawPartDF.select(col('device_id').alias('DeviceId'),
                                         col('topic').alias('Topic'),
                                         col('em_id').alias('EdgeModuleId'),
                                         col('storeid').alias('StoreId'),
                                         col('message_id').alias('MachineMessageId'),
                                         col('message_type').alias('TopMsgType'),
                                         col('body.type').alias('JsonMessageType'),
                                         col('body.body.index').alias('Index'),
                                         col('body.body.internalText').alias('ErrorInternalText'),
                                         col('body.body.module').alias('affectedmodule'),
                                         col('body.body.number').alias('ErroridNumber'),
                                         col('body.body.severity').alias('ErrorSeverity'),
                                         col('body.body.source').alias('Source'),
                                         col('body.body.stateful').alias('statefullerrorind'),
                                    #col('body.time').alias('time'),
                                    col('body.eventTime').alias('EdgeDateTimeStamp'),
                                    col('body.eventTimeLocal').alias('eventtimelocal'),
                                    col('body.version').alias('machineversion'),
                                    col('body.body.location.countryCode').alias('locationcountrycode'),
                                    col('body.body.location.description').alias('locationdescription'),
                                    col('body.body.location.language').alias('locationlanguage'),
                                    col('body.body.location.timeZone').alias('locationtimezone'),
                                    col('body.body.location.utcOffset').alias('locationutcoffset'),
                                    col('body.body.modules.id').getItem(0).alias('module1id'),
                                    col('body.body.modules.name').getItem(0).alias('module1name'),
                                    col('body.body.modules.id').getItem(1).alias('module2id'),
                                    col('body.body.modules.name').getItem(1).alias('module2name'),
                                    col('body.body.modules.id').getItem(2).alias('module3id'),
                                    col('body.body.modules.name').getItem(2).alias('module3name'),
                                    col('body.body.networkinterfaces.address').alias('networkinterfaceaddress'),
                                    col('body.body.networkinterfaces.name').alias('networkinterfacename'),
                                    col('body.body.software.version').alias('devicesoftwareversion'),
                                    col('body.body.recipe.category').alias('coffeerecipecategory'),
                                    col('body.body.recipe.paramcoffee.NumBrewCycles').alias('coffeeRecipeNumBrewCycles'),
                                    col('body.body.recipe.paramcoffee.Name').alias('CoffeeRecipeName'),
                                    col('body.body.recipe.paramcoffee.IsqExtrTime').alias('CoffeeRecipeIsqExtrTime'),
                                    col('body.body.recipe.paramcoffee.IsqPuckThickn').alias('CoffeeRecipeIsqPuckThickn'),
                                    col('body.body.recipe.paramcoffee.MaxExtrTime').alias('CoffeeRecipeMaxExtrTime'),
                                    col('body.body.recipe.paramcoffee.MaxValidExtrTime').alias('CoffeeRecipeMaxValidExtrTime'),
                                    col('body.body.recipe.paramcoffee.MinValidExtrTime').alias('CoffeeRecipeMinValidExtrTime'),
                                    col('body.body.recipe.paramcoffee.Module').alias('CoffeeRecipeModule'),
                                    col('body.body.recipe.paramcoffee.PostPress').alias('CoffeeRecipePostPress'),
                                    col('body.body.recipe.paramcoffee.PreBrew').alias('CoffeeRecipePreBrew'),
                                    col('body.body.recipe.paramcoffee.PressFactor').alias('CoffeeRecipePressFactor'),
                                    col('body.body.recipe.paramcoffee.RelaxTime').alias('CoffeeRecipeRelaxTime'),
                                    col('body.body.recipe.paramcoffee.TotalAmount').alias('CoffeeRecipeTotalAmount'),
                                    col('body.body.recipe.paramcoffee.UseExpChamber').alias('CoffeeExpansionChamberUsed'),
                                    col('body.body.recipe.paramcoffee.UseForISQ').alias('CoffeeUseCoffeeForISQAutomaticAdjustments'),
                                    col('body.body.recipe.paramcoffee.Valid').alias('CoffeeRecipeValid'),
                                    col('body.body.recipe.paramcoffee.grinder1.amount').alias('coffeeRecipeGrinder1Amount'),
                                    col('body.body.recipe.paramcoffee.grinder1.valid').alias('coffeeRecipeGrinder1ValidInd'),
                                    col('body.body.recipe.paramcoffee.grinder2.amount').alias('coffeeRecipeGrinder2Amount'),
                                    col('body.body.recipe.paramcoffee.grinder2.valid').alias('coffeeRecipeGrinder2ValidInd'),
                                    col('body.body.recipe.paramcoffee.grinder3.amount').alias('coffeeRecipeGrinder3Amount'),
                                    col('body.body.recipe.paramcoffee.grinder3.valid').alias('coffeeRecipeGrinder3ValidInd'),
                                    col('body.body.recipe.paramcoffeegrind.grinder1.amount').alias('CoffeeGrindRecipeGrinder1Amount'),
                                    col('body.body.recipe.paramcoffeegrind.grinder1.valid').alias('CoffeeGrindRecipeGrinder1Valid'),
                                    col('body.body.recipe.paramcoffeegrind.grinder2.amount').alias('CoffeeGrindRecipeGrinder2Amount'),
                                    col('body.body.recipe.paramcoffeegrind.grinder2.valid').alias('CoffeeGrindRecipeGrinder2Valid'),
                                    col('body.body.recipe.paramcoffeegrind.grinder3.amount').alias('CoffeeGrindRecipeGrinder3Amount'),
                                    col('body.body.recipe.paramcoffeegrind.grinder3.valid').alias('CoffeeGrindRecipeGrinder3Valid'),
                                    col('body.body.recipe.paramcoffeegrind.module').alias('CoffeeGrindRecipeModule'),
                                    col('body.body.recipe.paramcoffeegrind.valid').alias('CoffeeGrindRecipeValidInd'),
                                    col('body.body.recipe.paramsteam.autooff').alias('SteamRecipeAutoOffInd'),
                                    col('body.body.recipe.paramsteam.module').alias('SteamRecipeModule'),
                                    col('body.body.recipe.paramsteam.temperature').alias('SteamRecipeTemperature'),
                                    col('body.body.recipe.paramsteam.valid').alias('SteamRecipeValidInd'),
                                    col('body.body.result.resultcoffeerinse1.errorno').alias('CoffeeRinse1ResultErrorNo'),
                                    col('body.body.result.resultcoffeerinse1.name').alias('CoffeeRinse1ResultName'),
                                    col('body.body.result.resultcoffeerinse1.success').alias('CoffeeRinse1ResultSuccessInd'),
                                    col('body.body.result.resultcoffeerinse1.valid').alias('CoffeeRinse1ResultValidInd'),
                                    col('body.body.result.resultcoffeerinse1.wateramount').alias('CoffeeRinse1ResultWaterAmount'),
                                    col('body.body.result.resultcoffeerinse2.errorno').alias('CoffeeRinse2ResultErrorNo'),
                                    col('body.body.result.resultcoffeerinse2.name').alias('CoffeeRinse2ResultName'),
                                    col('body.body.result.resultcoffeerinse2.success').alias('CoffeeRinse2ResultSuccessInd'),
                                    col('body.body.result.resultcoffeerinse2.valid').alias('CoffeeRinse2ResultValidInd'),
                                    col('body.body.result.resultcoffeerinse2.wateramount').alias('CoffeeRinse2ResultWaterAmount'),
                                    col('body.body.result.resultsteam.autostop').alias('CoffeeSteamResultAutoInd'),
                                    col('body.body.result.resultsteam.duration').alias('CoffeeSteamResultDuration'),
                                    col('body.body.result.resultsteam.endtemp').alias('CoffeeSteamResultEndTemp'),
                                    col('body.body.result.resultsteam.errorno').alias('CoffeeSteamResultErrorNumber'),
                                    col('body.body.result.resultsteam.manualstop').alias('CoffeeSteamResultManualStopInd'),
                                    col('body.body.result.resultsteam.module').alias('CoffeeSteamResultModule'),
                                    col('body.body.result.resultsteam.success').alias('CoffeeSteamResultSuccessInd'),
                                    col('body.body.result.resultsteam.valid').alias('CoffeeSteamResultValidInd'),
                                    col('body.body.result.success').alias('CoffeeResultSuccessInd'),
                                    col('body.body.id').alias('CoffeeResultId'),
                                    col('body.body.name').alias('CoffeeName'),
                                    col('body.body.result.resultcoffee.brewchamberidx').alias('CoffeeResultBrewChamberIdx'),
                                    col('body.body.result.resultcoffee.brewcycle1.ExtrTime').alias('CoffeeResultBrewCycle1ExtractionTime'),
                                    col('body.body.result.resultcoffee.brewcycle1.PuckThicknAfterPr').alias('CoffeeResultBrewCycle1PuckThicknAfterPr'),
                                    col('body.body.result.resultcoffee.brewcycle1.PuckThicknAfterSq').alias('CoffeeResultBrewCycle1PuckThicknAfterSq'),
                                    col('body.body.result.resultcoffee.brewcycle1.Valid').alias('CoffeeResultBrewCycle1ValidInd'),
                                    col('body.body.result.resultcoffee.errorno').alias('CoffeeResultErrorNumber'),
                                    col('body.body.result.resultcoffee.grinder1.adjustment').alias('CoffeeResultGrinder1Adjustment'),
                                    col('body.body.result.resultcoffee.grinder1.calibfactor').alias('CoffeeResultGrinder1Calibration'),
                                    col('body.body.result.resultcoffee.grinder1.duration').alias('CoffeeResultGrinder1Duration'),
                                    col('body.body.result.resultcoffee.grinder1.rate').alias('CoffeeResultGrinder1Rate'),
                                    col('body.body.result.resultcoffee.grinder1.valid').alias('CoffeeResultGrinder1ValidInd'),
                                    col('body.body.result.resultcoffee.grinder2.adjustment').alias('CoffeeResultGrinder2Adjustment'),
                                    col('body.body.result.resultcoffee.grinder2.calibfactor').alias('CoffeeResultGrinder2Calibration'),
                                    col('body.body.result.resultcoffee.grinder2.duration').alias('CoffeeResultGrinder2Duration'),
                                    col('body.body.result.resultcoffee.grinder2.rate').alias('CoffeeResultGrinder2Rate'),
                                    col('body.body.result.resultcoffee.grinder2.valid').alias('CoffeeResultGrinder2ValidInd'),
                                    col('body.body.result.resultcoffee.grinder3.adjustment').alias('CoffeeResultGrinder3Adjustment'),
                                    col('body.body.result.resultcoffee.grinder3.calibfactor').alias('CoffeeResultGrinder3Calibration'),
                                    col('body.body.result.resultcoffee.grinder3.duration').alias('CoffeeResultGrinder3Duration'),
                                    col('body.body.result.resultcoffee.grinder3.rate').alias('CoffeeResultGrinder3Rate'),
                                    col('body.body.result.resultcoffee.grinder3.valid').alias('CoffeeResultGrinder3ValidInd'),
                                    col('body.body.result.resultcoffee.InletWaterCond').alias('CoffeeResultInletWaterConductivity'),
                                    col('body.body.result.resultcoffee.InletWaterTemp').alias('CoffeeResultInletWaterTemperature'),
                                    col('body.body.result.resultcoffee.Module').alias('CoffeeResultModule'),
                                    col('body.body.result.resultcoffee.Name').alias('CoffeeResultName'),
                                    col('body.body.result.resultcoffee.NumBrewCycles').alias('CoffeeResultNumBrewCycles'),
                                    col('body.body.result.resultcoffee.Success').alias('CoffeeResultProductSuccessInd'),
                                    col('body.body.result.resultcoffee.Valid').alias('CoffeeResultValidInd'),
                                    col('body.body.result.resultcoffee.WaterAmount').alias('CoffeeResultTotalWaterAmount'),
                                    col('body.body.result.resultcoffeeclean.ErrorNo').alias('CoffeeCleanResultErrorNo'),
                                    col('body.body.result.resultcoffeeclean.Module').alias('CoffeeCleanResultModule'),
                                    col('body.body.result.resultcoffeeclean.Success').alias('CoffeeCleanResultSuccessInd'),
                                    col('body.body.result.resultcoffeeclean.Valid').alias('CoffeeCleanResultValidInd'),
                                    col('body.body.result.resultcoffeeclean.WaterAmount').alias('CoffeeCleanResultWaterAmount')
                                    ).filter((lower(col('body.type')) == "error") | (lower(col('body.type')) == "machineinfo") | (lower(col('body.type')) == "productresult"))
                          )

# COMMAND ----------

#Machine Info Declarations
machineInfo_DF = spark.table("EDAP_PUB_STORE.ESPRESSO_MACHINERY_TELEMETRY_MACHINE_INFO").select("*")
expectedColumnCount_MachineInfo = 27
machineInfo_Dir = '/dbfs/mnt/data/published/Store/StoreEquipment/EspressoMachineTelemetryMachineInfo/ver=01/'
machineInfo_parquetFileInd = is_parquet_file(domain='Store', subDomain='StoreEquipment', dataProduct='EspressoMachineTelemetryMachineInfo', version='01')[0]
machineInfo_nonParquetFileInd = is_parquet_file(domain='Store', subDomain='StoreEquipment', dataProduct='EspressoMachineTelemetryMachineInfo', version='01')[1]
machineInfo_columnCount = len(machineInfo_DF.columns)
machineInfoRawDf = (spark.read.parquet('/mnt/data/raw/Store/StoreEquipment/EspressoMachineTelemetry/ver=01/2019/09/09/parquet/JsonMessageType=MachineInfo/'))


machineInfo_columns = ['Topic',
 'StoreId',
 'MessageVersion',
 'DeviceSoftwareVersion',
 'MachineEventUTCDateTime',
 'MachineEventLocalDateTime',
 'EdgeModuleId',
 'MachineMessageId',
 'LocationCountryCode',
 'LocationDescription',
 'LocationLanguage',
 'LocationTimeZone',
 'LocationUTCOffset',
 'Module1Id',
 'Module1Name',
 'Module2Id',
 'Module2Name',
 'Module3Id',
 'Module3Name',
 'NetworkInterfaceAddress',
 'NetworkInterfaceName',
 'DeviceId',
 'ETLLoadDate',
 'JsonMessageType',
 'ETLInsertDtm',
 'ETLUpdateDtm',
 'PublishedDate']

expectedSchema_machineInfo = """Topic: string, StoreId: string, MessageVersion: string, DeviceSoftwareVersion: string, MachineEventUTCDateTime: timestamp, MachineEventLocalDateTime: timestamp, EdgeModuleId: string, MachineMessageId: string, LocationCountryCode: string, LocationDescription: string, LocationLanguage: string, LocationTimeZone: string, LocationUTCOffset: string, Module1Id: string, Module1Name: string, Module2Id: string, Module2Name: string, Module3Id: string, Module3Name: string, NetworkInterfaceAddress: string, NetworkInterfaceName: string, DeviceId: string, ETLLoadDate: date, JsonMessageType: string, ETLInsertDtm: timestamp, ETLUpdateDtm: timestamp, PublishedDate: date"""

# COMMAND ----------

# DBTITLE 1,Machine Info Assertions
assert expectedColumnCount_MachineInfo == machineInfo_columnCount, "Expected Total to be {0}, but found {1}".format(str(expectedColumnCount_MachineInfo), machineInfo_columnCount)
for column in machineInfo_columns:
  assert columnCheck(machineInfo_DF,column), "The Column `{}` does not exist in dataset.".format(column)
assert is_delta_table(machineInfo_Dir), "Delta table is not defined under {}.".format(dir)
assert machineInfo_parquetFileInd > 0, "There are no parquet files in the directory, the landed files are not in the proper format."
assert machineInfo_nonParquetFileInd < 1, "There was files in the directory are not in the proper parquet format."
assert is_expected_schema(expectedSchema_machineInfo, machineInfo_DF), "There was an issue with the schema"
assert inc_machine_prepared_df.filter(lower(col("JsonMessageType")) == "machineinfo").count() == machineInfoRawDf.count(),"The file count is not the same between the json and parquet formats"

# COMMAND ----------

#Error Declarations
error_table = "select * from EDAP_PUB_STORE.ESPRESSO_MACHINERY_TELEMETRY_ERROR"
error_DF = spark.sql(error_table)
expectedColumnCount_Error = 20
error_Dir = '/dbfs/mnt/data/published/Store/StoreEquipment/EspressoMachineTelemetryError/ver=01/'
error_parquetFileInd = is_parquet_file(domain='Store', subDomain='StoreEquipment', dataProduct='EspressoMachineTelemetryError', version='01')[0]
error_nonParquetFileInd = is_parquet_file(domain='Store', subDomain='StoreEquipment', dataProduct='EspressoMachineTelemetryError', version='01')[1]
error_columnCount = len(error_DF.columns)

errorRawDf = (spark.read.parquet('/mnt/data/raw/Store/StoreEquipment/EspressoMachineTelemetry/ver=01/2019/09/09/parquet/JsonMessageType=Error/'))


error_columns = ['Topic',
 'StoreId',
 'Index',
 'ErrorInternalText',
 'Affectedmodule',
 'ErrorIdNumber',
 'ErrorSeverity',
 'Source',
 'StatefullErrorInd',
 'MachineEventUTCDatetime',
 'MachineEventLocalDateTime',
 'EdgeModuleId',
 'MachineMessageId',
 'DeviceId',
 'MessageVersion',
 'ETLLoadDate',
 'JsonMessageType',
 'ETLInsertDtm',
 'ETLUpdateDtm',
 'PublishedDate']

expectedSchema_Error = """Topic: string, StoreId: string, Index: int, ErrorInternalText: string, Affectedmodule: string, ErrorIdNumber: int, ErrorSeverity: int, Source: string, StatefullErrorInd: int, MachineEventUTCDatetime: timestamp, MachineEventLocalDateTime: timestamp, EdgeModuleId: string, MachineMessageId: string, DeviceId: string, MessageVersion: string, ETLLoadDate: date, JsonMessageType: string, ETLInsertDtm: timestamp, ETLUpdateDtm: timestamp, PublishedDate: date"""

# COMMAND ----------

# DBTITLE 1,Error Assertions
assert expectedColumnCount_Error == error_columnCount, "Expected Total to be {0}, but found {1}".format(str(expectedColumnCount_Error), error_columnCount)
for column in error_columns:
  assert columnCheck(error_DF,column), "The Column `{}` does not exist in dataset.".format(column)
assert is_delta_table(error_Dir), "Delta table is not defined under {}.".format(dir)
assert error_parquetFileInd > 0, "There are no parquet files in the directory, the landed files are not in the proper format."
assert error_nonParquetFileInd < 1, "There was files in the directory are not in the proper parquet format."
assert is_expected_schema(expectedSchema_Error,error_DF), "There was an issue with the schema"
assert inc_machine_prepared_df.filter(lower(col("JsonMessageType")) == "error").count() == errorRawDf.count(),"The file count is not the same between the json and parquet formats"

# COMMAND ----------

#Product Result Declarations
productResult_DF = spark.table("EDAP_PUB_STORE.ESPRESSO_MACHINERY_TELEMETRY_PRODUCT_RESULT").select("*")
expectedColumnCount_ProductResult = 113
productResult_Dir = '/dbfs/mnt/data/published/Store/StoreEquipment/EspressoMachineTelemetryProductResult/ver=01/'
productResult_parquetFileInd = is_parquet_file(domain='Store', subDomain='StoreEquipment', dataProduct='EspressoMachineTelemetryProductResult', version='01')[0]
productResult_nonParquetFileInd = is_parquet_file(domain='Store', subDomain='StoreEquipment', dataProduct='EspressoMachineTelemetryProductResult', version='01')[1]
productResult_columnCount = len(productResult_DF.columns)
productResultRawDf = (spark.read.parquet('/mnt/data/raw/Store/StoreEquipment/EspressoMachineTelemetry/ver=01/2019/09/09/parquet/JsonMessageType=ProductResult/'))

productResult_columns = ['DeviceId',
 'Topic',
 'StoreId',
 'CoffeeRecipeCategory',
 'CoffeeRecipeGrinder1Amount',
 'CoffeeRecipeGrinder1ValidInd',
 'CoffeeRecipeGrinder2Amount',
 'CoffeeRecipeGrinder2ValidInd',
 'CoffeeRecipeGrinder3Amount',
 'CoffeeRecipeGrinder3ValidInd',
 'CoffeeRecipeIsqExtrTime',
 'CoffeeRecipeIsqPuckThickn',
 'CoffeeRecipeMaxExtrTime',
 'CoffeeRecipeMaxValidExtrTime',
 'CoffeeRecipeMinValidExtrTime',
 'CoffeeRecipeModule',
 'CoffeeRecipeName',
 'CoffeeRecipeNumBrewCycles',
 'CoffeeRecipePostPress',
 'CoffeeRecipePreBrew',
 'CoffeeRecipePressFactor',
 'CoffeeRecipeRelaxTime',
 'CoffeeRecipeTotalAmount',
 'CoffeeExpansionChamberUsed',
 'CoffeeUseCoffeeForISQAutomaticAdjustments',
 'CoffeeRecipeValid',
 'CoffeeGrindRecipeGrinder1Amount',
 'CoffeeGrindRecipeGrinder1Valid',
 'CoffeeGrindRecipeGrinder2Amount',
 'CoffeeGrindRecipeGrinder2Valid',
 'CoffeeGrindRecipeGrinder3Amount',
 'CoffeeGrindRecipeGrinder3Valid',
 'CoffeeGrindRecipeModule',
 'CoffeeGrindRecipeValidInd',
 'SteamRecipeAutoOffInd',
 'SteamRecipeModule',
 'SteamRecipeTemperature',
 'SteamRecipeValidInd',
 'ProductResultID',
 'ProductResultName',
 'CoffeeResultBrewChamberIdx',
 'CoffeeResultBrewCycle1ExtractionTime',
 'CoffeeResultBrewCycle1PuckThicknAfterPr',
 'CoffeeResultBrewCycle1PuckThicknAfterSq',
 'CoffeeResultBrewCycle1ValidInd',
 'CoffeeResultErrorNumber',
 'CoffeeResultGrinder1Adjustment',
 'CoffeeResultGrinder1Calibration',
 'CoffeeResultGrinder1Duration',
 'CoffeeResultGrinder1Rate',
 'CoffeeResultGrinder1ValidInd',
 'CoffeeResultGrinder2Adjustment',
 'CoffeeResultGrinder2Calibration',
 'CoffeeResultGrinder2Duration',
 'CoffeeResultGrinder2Rate',
 'CoffeeResultGrinder2ValidInd',
 'CoffeeResultGrinder3Adjustment',
 'CoffeeResultGrinder3Calibration',
 'CoffeeResultGrinder3Duration',
 'CoffeeResultGrinder3Rate',
 'CoffeeResultGrinder3ValidInd',
 'CoffeeResultInletWaterConductivity',
 'CoffeeResultInletWaterTemperature',
 'CoffeeResultModule',
 'CoffeeResultName',
 'CoffeeResultNumBrewCycles',
 'CoffeeResultProductSuccessInd',
 'CoffeeResultValidInd',
 'CoffeeResultTotalWaterAmount',
 'CoffeeCleanResultErrorNo',
 'CoffeeCleanResultModule',
 'CoffeeCleanResultSuccessInd',
 'CoffeeCleanResultValidInd',
 'CoffeeCleanResultWaterAmount',
 'CoffeeRinse1ResultErrorNo',
 'CoffeeRinse1ResultName',
 'CoffeeRinse1ResultSuccessInd',
 'CoffeeRinse1ResultValidInd',
 'CoffeeRinse1ResultWaterAmount',
 'CoffeeRinse2ResultErrorNo',
 'CoffeeRinse2ResultName',
 'CoffeeRinse2ResultSuccessInd',
 'CoffeeRinse2ResultValidInd',
 'CoffeeRinse2ResultWaterAmount',
 'CoffeeSteamResultAutoInd',
 'CoffeeSteamResultDuration',
 'CoffeeSteamResultEndTemp',
 'CoffeeSteamResultErrorNumber',
 'CoffeeSteamResultManualStopInd',
 'CoffeeSteamResultModule',
 'CoffeeSteamResultSuccessInd',
 'CoffeeSteamResultValidInd',
 'CoffeeResultSuccessInd',
 'MachineEventUTCDateTime',
 'MachineEventLocalDateTime',
 'EdgeModuleId',
 'MachineMessageId',
 'MessageVersion',
 'ETLLoadDate',
 'JsonMessageType',
 'ETLInsertDtm',
 'ETLUpdateDtm',
 'PublishedDate',
 'TotalGrinderRate',
 'TotalGrinderCalibrationFactor',
 'TotalGrinderDuration',
 'TotalGrinderAdjustment',
 'ShotQuality',
 'ProductType',
 'ShotSize',
 'ShotStyle',
 'Dose',
 'BeanName']

expectedError_Schema = """DeviceId: string, Topic: string, StoreId: string, CoffeeRecipeCategory: string, CoffeeRecipeGrinder1Amount: int, CoffeeRecipeGrinder1ValidInd: int, CoffeeRecipeGrinder2Amount: int, CoffeeRecipeGrinder2ValidInd: int, CoffeeRecipeGrinder3Amount: int, CoffeeRecipeGrinder3ValidInd: int, CoffeeRecipeIsqExtrTime: int, CoffeeRecipeIsqPuckThickn: double, CoffeeRecipeMaxExtrTime: int, CoffeeRecipeMaxValidExtrTime: int, CoffeeRecipeMinValidExtrTime: int, CoffeeRecipeModule: string, CoffeeRecipeName: string, CoffeeRecipeNumBrewCycles: int, CoffeeRecipePostPress: int, CoffeeRecipePreBrew: int, CoffeeRecipePressFactor: double, CoffeeRecipeRelaxTime: int, CoffeeRecipeTotalAmount: int, CoffeeExpansionChamberUsed: int, CoffeeUseCoffeeForISQAutomaticAdjustments: string, CoffeeRecipeValid: int, CoffeeGrindRecipeGrinder1Amount: int, CoffeeGrindRecipeGrinder1Valid: int, CoffeeGrindRecipeGrinder2Amount: int, CoffeeGrindRecipeGrinder2Valid: int, CoffeeGrindRecipeGrinder3Amount: int, CoffeeGrindRecipeGrinder3Valid: int, CoffeeGrindRecipeModule: string, CoffeeGrindRecipeValidInd: int, SteamRecipeAutoOffInd: int, SteamRecipeModule: string, SteamRecipeTemperature: double, SteamRecipeValidInd: int, ProductResultID: string, ProductResultName: string, CoffeeResultBrewChamberIdx: string, CoffeeResultBrewCycle1ExtractionTime: double, CoffeeResultBrewCycle1PuckThicknAfterPr: double, CoffeeResultBrewCycle1PuckThicknAfterSq: double, CoffeeResultBrewCycle1ValidInd: int, CoffeeResultErrorNumber: int, CoffeeResultGrinder1Adjustment: int, CoffeeResultGrinder1Calibration: double, CoffeeResultGrinder1Duration: double, CoffeeResultGrinder1Rate: double, CoffeeResultGrinder1ValidInd: int, CoffeeResultGrinder2Adjustment: int, CoffeeResultGrinder2Calibration: double, CoffeeResultGrinder2Duration: double, CoffeeResultGrinder2Rate: double, CoffeeResultGrinder2ValidInd: int, CoffeeResultGrinder3Adjustment: int, CoffeeResultGrinder3Calibration: double, CoffeeResultGrinder3Duration: double, CoffeeResultGrinder3Rate: double, CoffeeResultGrinder3ValidInd: int, CoffeeResultInletWaterConductivity: int, CoffeeResultInletWaterTemperature: double, CoffeeResultModule: string, CoffeeResultName: string, CoffeeResultNumBrewCycles: int, CoffeeResultProductSuccessInd: int, CoffeeResultValidInd: int, CoffeeResultTotalWaterAmount: double, CoffeeCleanResultErrorNo: int, CoffeeCleanResultModule: string, CoffeeCleanResultSuccessInd: int, CoffeeCleanResultValidInd: int, CoffeeCleanResultWaterAmount: double, CoffeeRinse1ResultErrorNo: int, CoffeeRinse1ResultName: string, CoffeeRinse1ResultSuccessInd: int, CoffeeRinse1ResultValidInd: int, CoffeeRinse1ResultWaterAmount: double, CoffeeRinse2ResultErrorNo: int, CoffeeRinse2ResultName: string, CoffeeRinse2ResultSuccessInd: int, CoffeeRinse2ResultValidInd: int, CoffeeRinse2ResultWaterAmount: double, CoffeeSteamResultAutoInd: int, CoffeeSteamResultDuration: int, CoffeeSteamResultEndTemp: double, CoffeeSteamResultErrorNumber: int, CoffeeSteamResultManualStopInd: int, CoffeeSteamResultModule: string, CoffeeSteamResultSuccessInd: int, CoffeeSteamResultValidInd: int, CoffeeResultSuccessInd: int, MachineEventUTCDateTime: timestamp, MachineEventLocalDateTime: timestamp, EdgeModuleId: string, MachineMessageId: string, MessageVersion: string, ETLLoadDate: date, JsonMessageType: string, ETLInsertDtm: timestamp, ETLUpdateDtm: timestamp, PublishedDate: date, TotalGrinderRate: double, TotalGrinderCalibrationFactor: double, TotalGrinderDuration: double, TotalGrinderAdjustment: int, ShotQuality: string, ProductType: string, ShotSize: string, ShotStyle: string, Dose: string, BeanName: string"""

# COMMAND ----------

# DBTITLE 1,Product Result Assertions
assert expectedColumnCount_ProductResult == productResult_columnCount, "Expected Total to be {0}, but found {1}".format(str(expectedColumnCount_ProductResult), productResult_columnCount)
for column in productResult_columns:
  assert columnCheck(productResult_DF,column), "The Column `{}` does not exist in dataset.".format(column)
assert is_delta_table(productResult_Dir), "Delta table is not defined under {}.".format(dir)
assert productResult_parquetFileInd > 0, "There are no parquet files in the directory, the landed files are not in the proper format."
assert productResult_nonParquetFileInd < 1, "There was files in the directory are not in the proper parquet format."
assert is_expected_schema(expectedError_Schema,productResult_DF), "There was an issue with the schema"
assert inc_machine_prepared_df.filter(lower(col("JsonMessageType")) == "productresult").count() == productResultRawDf.count(),"The file count is not the same between the json and parquet formats"
