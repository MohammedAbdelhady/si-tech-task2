const fs = require("fs");
const csvParser = require("csv-parser");
const prompt = require("prompt");
const headers = ["id", "area", "productName", "quantity", "brand"];
const inputFolder = "inputFiles";
const outputFolder = "outputFiles";


createInputAndOutputFolderIfNotExist();

const properties = [
  {
    name: "fileName",
    validator: /^[a-zA-Z0-9\s-_]+.csv$/,
    warning: "fileName must a valid csv file e.g. input_file_name.csv",
  },
];

prompt.start();

prompt.get(properties, function (err, result) {
  if (err) {
    console.log(err.message);
    process.exit(0);
  }

  const inputFileName = result.fileName;

  const productsQuantityMappedByProductName = {};
  let totalOrders = 0;

  const readStream = fs
    .createReadStream(`${inputFolder}/${inputFileName}`)
    .on("error", (error) => {
      console.log(error.message);
    });

  readStream
    .pipe(csvParser({ headers }))
    .on("data", (record) => {
      const { quantity, productName, brand } = record;

      if (!productsQuantityMappedByProductName[productName]) {
        productsQuantityMappedByProductName[productName] = {
          quantity: 0,
          ordersByBrand: {},
        };
      }

      if (
        !productsQuantityMappedByProductName[productName].ordersByBrand[brand]
      ) {
        productsQuantityMappedByProductName[productName].ordersByBrand[
          brand
        ] = 0;
      }

      productsQuantityMappedByProductName[productName].ordersByBrand[
        brand
      ] += 1;

      productsQuantityMappedByProductName[productName].quantity +=
        parseInt(quantity);

      totalOrders += 1;
    })
    .on("end", () => {
      const outputFile1WritableStream = fs.createWriteStream(
        `${outputFolder}/0_${inputFileName}`,
      );
      const outputFile2WritableStream = fs.createWriteStream(
        `${outputFolder}/1_${inputFileName}`
      );

      for ([productName, productStats] of Object.entries(
        productsQuantityMappedByProductName
      )) {
        outputFile1WritableStream.write(
          `${productName},${productStats.quantity / totalOrders}\n`
        );

        let popularBrand = null;
        let maxOrdersForTheBrand = 0;
        for ([brand, numberOfOrders] of Object.entries(
          productStats.ordersByBrand
        )) {
          if (maxOrdersForTheBrand < numberOfOrders) {
            maxOrdersForTheBrand = numberOfOrders;
            popularBrand = brand;
          }
        }

        outputFile2WritableStream.write(`${productName},${popularBrand}\n`);
      }

      console.log(
        `Files have been written to disk successfully you can find them inside "${outputFolder}" folder`
      );
    })
    .on("error", (error) => {
      console.log(error.message);
    });
});

function createInputAndOutputFolderIfNotExist() {
  fs.mkdirSync(inputFolder, { recursive: true });
  fs.mkdirSync(outputFolder, { recursive: true });
}

