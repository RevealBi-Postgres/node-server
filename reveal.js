var express = require('express');
var reveal = require('reveal-sdk-node');
var cors = require('cors');
const fs = require("fs");
const { pipeline } = require("stream");
const { promisify } = require('util');
const pipelineAsync = promisify(pipeline);

const dashboardDefaultDirectory = "dashboards";
const dashboardDirectory = "dashboards-tenant1";

const app = express();
app.use(cors()); 

// Step 1: User context provider
const userContextProvider = (request) => {
  console.log('All request headers:', request.headers); 
  const userId = request.headers['x-header-customerid']; 
  if (!userId) {
    console.warn('x-header-customerId is not found in headers');
  }
  var props = new Map();
  props.set("userId", userId); 
  return new reveal.RVUserContext(userId, props);
};

// Step 2: Data source provider
const dataSourceProvider = async (userContext, dataSource) => {
  if (dataSource instanceof reveal.RVPostgresDataSource) {
      dataSource.host = "s0106docker2.infragistics.local";
      dataSource.database = "Northwind";
      dataSource.schema = "public";
  } else if (dataSource instanceof reveal.RVSnowflakeDataSource) {
    dataSource.account = "v900923";
    dataSource.host = "v900923.snowflakecomputing.com";
    dataSource.database = "SNOWFLAKE_SAMPLE_DATA";		
  } else {
    return null;
  }
  return dataSource;
};

// Step 3: Authentication provider
const authenticationProvider = async (userContext, dataSource) => {  
  if (dataSource instanceof reveal.RVPostgresDataSource || dataSource instanceof reveal.RVSnowflakeDataSource) {
    return new reveal.RVUsernamePasswordDataSourceCredential("jason", "jason"); 
  }
}

// Step 4: Data source item provider
const dataSourceItemProvider = async (userContext, dataSourceItem) => {

//console.log(`Loading data source item ${dataSourceItem.id}`);

  if (dataSourceItem instanceof reveal.RVLocalFileDataSourceItem)
    {
      dataSourceItem.Uri = "local:/" + dataSourceItem.Id + ".xlsx";
    }

  if (dataSourceItem instanceof reveal.RVPostgresDataSourceItem) {

  //  console.log(`Loading data source Table ${dataSourceItem.table}`);

    if (dataSourceItem.table == "customers") {
      dataSourceItem.customQuery = "SELECT * FROM customers where customerid = '" + userContext.properties.get("userId") + "'";
     // console.log(dataSourceItem.customQuery);
    }

    if (dataSourceItem.id == "customers-dynamic") {
      dataSourceItem.customQuery = "SELECT `URID`, `Procedure`, `Time` FROM `devAppLog`";
    }

    if (dataSourceItem.id == "customers-alfki-function") {
      dataSourceItem.schema = "public";
      dataSourceItem.functionName = "customerordersf";
      dataSourceItem.functionParameters = { custid: userContext.properties.get("userId") };
    }

    if (dataSourceItem.id == "ordersqry") {
      dataSourceItem.table = "OrdersQry";
    }
  }
  await dataSourceProvider(userContext, dataSourceItem.dataSource);
  return dataSourceItem;
}

// Step 5: Dashboard provider
const dashboardProvider = async (userContext, dashboardId) => {
  console.log(`Loading dashboard ${dashboardId}`);
  return fs.createReadStream(`${dashboardDefaultDirectory}/${dashboardId}.rdash`);
}

// Step 6: Dashboard storage provider
const dashboardStorageProvider = async (userContext, dashboardId, stream) => {
  console.log(`Saving dashboard ${dashboardId}`);
  const userId = userContext.properties.get("userId");
  let savePath;

  if (userId === 'ALFKI') {
    savePath = `${dashboardDefaultDirectory}/${dashboardId}.rdash`;
    console.log(`Saving dashboard ${dashboardId} for user ${userId} to ${savePath}`);
  } else {
    savePath = `${dashboardDefaultDirectory}/${dashboardId}.rdash`;
  }

  await pipelineAsync(stream, fs.createWriteStream(savePath));
};

// Reveal options
const revealOptions = {
  userContextProvider: userContextProvider,
  authenticationProvider: authenticationProvider,
  dataSourceProvider: dataSourceProvider,
  dataSourceItemProvider: dataSourceItemProvider,
  dashboardProvider: dashboardProvider,
  dashboardStorageProvider: dashboardStorageProvider,
  localFileStoragePath: "data"
};

// Export the middleware for reveal
module.exports = reveal(revealOptions);