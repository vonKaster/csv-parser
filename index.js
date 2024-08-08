const fs = require('fs');
const csv = require('csv-parser');
const fastcsv = require('fast-csv');

function removeDuplicates(inputFilePath, outputFilePath, column, delimiter = ';') {
  const records = [];
  const uniqueRecords = new Set();
  let rowCount = 0;

  // Leer el archivo .csv
  fs.createReadStream(inputFilePath)
    .pipe(csv({ separator: delimiter }))
    .on('data', (row) => {
      rowCount++;
      if (rowCount <= 5) { // Imprime las primeras 5 filas para verificar
        console.log(row);
      }

      if (row.hasOwnProperty(column)) {
        const columnValue = row[column].trim();

        if (!uniqueRecords.has(columnValue)) {
          uniqueRecords.add(columnValue);
          records.push(row);
        }
      } else {
        console.error(`La columna "${column}" no existe en la fila:`, row);
      }
    })
    .on('end', () => {
      console.log(`Total de registros leídos: ${rowCount}`);
      console.log(`Total de registros únicos: ${records.length}`);

      const ws = fs.createWriteStream(outputFilePath);
      fastcsv
        .write(records, { headers: true, delimiter: delimiter })
        .pipe(ws)
        .on('finish', () => {
          console.log(`Archivo procesado y guardado en ${outputFilePath}`);
        });
    });
}

// Uso: pasar los nombres de archivo de entrada y salida, y la columna a verificar
const inputFilePath = 'input.csv';
const outputFilePath = 'output.csv';
const column = 'MAIL_TRAMIX';

removeDuplicates(inputFilePath, outputFilePath, column);
