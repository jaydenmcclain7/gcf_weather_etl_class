const {Storage} = require('@google-cloud/storage');
const csv = require('csv-parser');

const {BigQuery} = require('@google-cloud/bigquery');

const bq = new BigQuery();
const datasetId = 'weather_etl';
const tableId='weather_etl';


exports.readObservation = (file, context) => {
    // console.log(`  Event: ${context.eventId}`);
    // console.log(`  Event Type: ${context.eventType}`);
    // console.log(`  Bucket: ${file.bucket}`);
    // console.log(`  File: ${file.name}`);

    const gcs = new Storage();

    const dataFile = gcs.bucket(file.bucket).file(file.name);

    dataFile.createReadStream()
    .on('error', () => {
        // Handle an error
        console.error(error);
    })
    .pipe(csv())
    .on('data', (row) => {
        // Log row data
        // console.log(row);
        transNull(row);
        divTen(row);
        printDict(row);
        console.log(`Station : ${file.name.split('.csv',1)}`);
        writeToBq(row, file);
    })
    .on('end', () => {
        // Handle end of CSV
        console.log('End!');
    })
}

// HELPER FUNCTIONS

function printDict(row) {
    for (let key in row) {
        // console.log(key + ' : ' + row[key]);
        console.log(`${key} : ${row[key]}`);
    }
}

const transNull = (row)=>{
    for (let key in row){
        if(row[key]==-9999){
            row[key]=null;
        }
    }
}

const divTen = (row) =>{
    for (let key in row){
        if(row[key]!=null){
            if(key=='airtemp'||key=='dewpoint'||key=='pressure'||key=='windspeed'||key=='precip1hour'||key=='precip6hour'){
                row[key]/=10;
            }
        }
    }
}

async function writeToBq(row, file){
    let rows = [];
    let obj ={};
    for(let key in row){
        switch (key){
            case 'year':
                obj.year=row[key];
                break;
            case 'month':
                obj.month=row[key];
                break;
            case 'day':
                obj.day=row[key];
                break;
            case 'hour':
                obj.hour=row[key];
                break;
            case 'winddirection':
                obj.winddirection=row[key];
                break;
            case 'sky':
                obj.sky=row[key];
                break;
            case 'airtemp':
                obj.airtemp=row[key];
                break;
            case 'dewpoint':
                obj.dewpoint=row[key];
                break;
            case 'pressure':
                obj.pressure=row[key];
                break;
            case 'windspeed':
                obj.windspeed=row[key];
                break;
            case 'precip1hour':
                obj.precip1hour=row[key];
                break;
            case 'precip6hour':
                obj.precip6hour=row[key];
                break;
        }
    }
    obj.station = `${file.name.split('.csv',1)}`;
    rows.push(obj);

    await bq
    .dataset(datasetId)
    .table(tableId)
    .insert(rows)
    .then(()=>{
        rows.forEach ((row)=>{
            console.log(`Inserted: ${row}`);
        })
    })
    .catch((err)=>{`ERROR: ${err}`})
}