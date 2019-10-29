const nlp = require('compromise');
const mysql = require('mysql2/promise');
const squel = require('squel');
const _ = require('lodash');
const { Maybe } = require('monet');
const csv = require('async-csv');
const fs = require('fs').promises;
const { createObjectCsvWriter } = require('csv-writer');

const csvWriter = createObjectCsvWriter({
    path: 'compromise-results8.csv',
    header: [
        {id: 'title', title: 'title'},
        {id: 'new_man', title: 'new_man'},
        {id: 'old_man', title: 'old_man'},
        {id: 'new_model', title: 'new_model'},
        {id: 'old_model', title: 'old_model'}
    ]
});

const sortByWordCountAndLength = (arr)=> {
    arr.map(item=>{
        const wordCount = (item && item.name && item.name.split(' ').length) || 0;
        item.length = (item && item.name && item.name.length) || 0;
        item.wordCount = wordCount;
        return item;

    });
    return _.sortBy(arr, ['wordCount', 'length']).reverse();
};

const findManufacturers = async (connection, searches) => {
        const query = squel.select({ autoQuoteFieldNames: true })
            .field('m.*')
            .distinct()
            .from('manufacturers', 'm')
            .left_join('manufacturer_synonyms', 'ms', 'm.id = ms.manufacturer_id')
            .where('m.name IN ? OR ms.synonym IN ?', searches.ngrams, searches.ngrams);
        const { text, values } = query.toParam();
        const [rows] = await connection.execute(text, values);
        searches.manufacturers = sortByWordCountAndLength(rows);

    return searches;
};

const findManWithUnion = async (connection, searches)=> {

    const manufacturerQuery = squel.select().fields({
        id:   'id',
        name: 'text'
    }).from('manufacturers').where('name IN ?', searches.ngrams);

    const synonymQuery = squel.select().fields({
        id:      'id',
        synonym: 'text'
    }).from('manufacturer_synonyms').where('synonym in ?', searches.ngrams);

    const query = squel.select().from(manufacturerQuery.union(synonymQuery), 'm');
    const { text, values } = query.toParam();
    const [rows] = await connection.execute(text, values);

    searches.manufacturers = sortByWordCountAndLength(rows);

    return searches;
}

const findProdWithUnion = async (connection, searches)=>{

    const manufacturerIds = searches.manufacturers.map(m => m.id);
    if(_.isEmpty(manufacturerIds)) {
        return searches;
    }

    const productQuery = squel.select().fields({
        id:    'id',
        model: 'text'
    }).from('products').where('manufacturer_id IN ? AND model IN ?', manufacturerIds, searches.ngrams);
    const synonymQuery = squel.select().fields({
        'p.id':      'id',
        'ps.name': 'text'
    })
        .from('product_synonyms', 'ps')
        .join('products', 'p', 'p.id = ps.product_id')
        .where('p.manufacturer_id in ? AND ps.name IN ?', manufacturerIds, searches.ngrams);

    const query = squel.select().from(productQuery.union(synonymQuery), 'p');
    const { text, values } = query.toParam();
    const [rows] = await connection.execute(text, values);

    searches.products = rows;

    return searches;
}

const findProducts = async (connection, searches) => {
    // const manufacturerIds = searches.reduce((acc, s) => acc.concat(s.manufacturers.map(m => m.id)), []);

    const manufacturerIds = searches.manufacturers.map(m => m.id);
    if(_.isEmpty(manufacturerIds)) {
        return searches;
    }
    // as of now, that is sequentially, can be in parallel
        const query = squel.select({ autoQuoteFieldNames: true })
            .field('p.*')
            .distinct()
            .from('products', 'p')
            .left_join('product_synonyms', 'ps', 'p.id = ps.product_id')
            .where('p.manufacturer_id IN ? AND (p.model IN ? OR ps.name IN ?)', manufacturerIds, searches.ngrams, searches.ngrams);
        const { text, values } = query.toParam();
        const [rows, fields] = await connection.execute(text, values);
        searches.products = rows;

    return searches;
};

const getManufacturer = (id, searches) => {
    // return _.chain(searches).map('manufacturers').flatten().find(_.matchesProperty('id', id)).value();
    return _.find(searches.manufacturers, { 'id': id});
};

const getFirstManufacturer = (searches) => {
    return searches.manufacturers[0];
    // return _.chain(searches).map('manufacturers').flatten().head().value();
};

function filter(newX, oldX){
    if(!newX && !oldX){
        return false;
    }
    if (newX == oldX) {
        return false;
    }
    return true;
}

(async () => {
    const connection = await mysql.createConnection({
        "host": "product-api-production-1.cywimmwjbsdz.eu-west-1.rds.amazonaws.com",
        "user": "api",
        "password": "oOkmOvaMfaVKVob",
        "database": "api"
    });
    const results = [];

    const csvString = await fs.readFile('./test1k.csv', 'utf-8');

    const rows = await csv.parse(csvString);

    console.time('xyz');
    for(let i=1; i<rows.length; i++){
        console.log('Row:', i);

        const searches = {};
        const processedText = rows[i];

        const ngrams = nlp(processedText[0]).ngrams( {max:6}).out("offsets");
        const allNgrams = ngrams.map(i=>i.text.trim()).concat(ngrams.map(i=>i.normal.trim()))
        searches.ngrams = allNgrams;

        try{
            const searchesWithManufacturer = await findManWithUnion(connection, searches);

            const searchesWithProducts = await findProdWithUnion(connection, searchesWithManufacturer);

            // really just take the first product?
            const product = _.isEmpty(searchesWithProducts.products) ? undefined : searchesWithProducts.products[0];
            const manufacturer = product ? getManufacturer(product.manufacturer_id, searches) : getFirstManufacturer(searches);

            if(filter((manufacturer || {}).id, processedText[1]) || filter((product || {}).id, processedText[2])){
                results.push({
                    title: processedText[0],
                    new_man: (manufacturer || {}).id,
                    old_man: processedText[1],
                    new_model: (product || {}).id,
                    old_model: processedText[2]
                });
            }

        }
        catch (err) {
            console.log('err', err);
        }
    }
    console.timeEnd('xyz');

    connection.close();

    csvWriter
        .writeRecords(results)
        .then(()=> console.log('The CSV file was written successfully'));


})();
