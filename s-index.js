const nlp = require('compromise');
const mysql = require('mysql2/promise');
const squel = require('squel');
const _ = require('lodash');
const { Maybe } = require('monet');
const csv = require('async-csv');
const fs = require('fs').promises;
const { createObjectCsvWriter } = require('csv-writer');

const csvWriter = createObjectCsvWriter({
    path: 'compromise-results.csv',
    header: [
        {id: 'title', title: 'title'},
        {id: 'new_man', title: 'new_man'},
        {id: 'old_man', title: 'old_man'},
        {id: 'new_model', title: 'new_model'},
        {id: 'old_model', title: 'old_model'}
    ]
});
const findManufacturers = async (connection, searches) => {
    for (const s of searches) {
        const query = squel.select({ autoQuoteFieldNames: true })
            .field('m.*')
            .distinct()
            .from('manufacturers', 'm')
            .left_join('manufacturer_synonyms', 'ms', 'm.id = ms.manufacturer_id')
            .where('m.name IN ? OR ms.synonym IN ?', s.ngrams, s.ngrams);
        const { text, values } = query.toParam();
        const [rows] = await connection.execute(text, values);
        s.manufacturers = rows;
    }

    return searches;
};

const findProducts = async (connection, searches) => {
    const manufacturerIds = searches.reduce((acc, s) => acc.concat(s.manufacturers.map(m => m.id)), []);

    if(_.isEmpty(manufacturerIds)) {
        return searches;
    }
    // as of now, that is sequentially, can be in parallel
    for (const s of searches) {

        const query = squel.select({ autoQuoteFieldNames: true })
            .field('p.*')
            .distinct()
            .from('products', 'p')
            .left_join('product_synonyms', 'ps', 'p.id = ps.product_id')
            .where('p.manufacturer_id IN ? AND (p.model IN ? OR ps.name IN ?)', manufacturerIds, s.ngrams, s.ngrams);
        const { text, values } = query.toParam();
        const [rows, fields] = await connection.execute(text, values);
        s.products = rows;
    }

    return searches;
};

const getManufacturer = (id, searches) => {
    return _.chain(searches).map('manufacturers').flatten().find(_.matchesProperty('id', id)).value();
};

const getFirstManufacturer = (searches) => {
    return _.chain(searches).map('manufacturers').flatten().head().value();
};
function sleep(ms){
    return new Promise(resolve=>{
        setTimeout(resolve,ms)
    })
}
(async () => {
    const connection = await mysql.createConnection({
       //TODO
    });
    const results = [];

    const csvString = await fs.readFile('./samples.csv', 'utf-8');

    const rows = await csv.parse(csvString);

    for(let i=1; i<rows.length; i++){
        const searches = [];
        const processedText = rows[i];
        const ngrams = nlp(processedText[0]).ngrams().out("offsets");

        // get both version from ngrams (text and normal)
        searches.push({
            ngrams: ngrams.map(i=>i.text.trim())
        });

        searches.push({
            ngrams: ngrams.map(i=>i.normal.trim())
        });
        const filtered = searches.filter(x=>!_.isEmpty(x.ngrams));

        try{
            const searchesWithManufacturer = await findManufacturers(connection, filtered);
            const searchesWithProducts = await findProducts(connection, searchesWithManufacturer);

            // really just take the first product?
            const product = Maybe.fromUndefined(searchesWithProducts.find(({ products }) => products && products.length > 0)).map(x => x.products[0]).getOrElse(null);
            const manufacturer = product ? getManufacturer(product.manufacturer_id, filtered) : getFirstManufacturer(filtered);

            results.push({
                title: processedText[0],
                new_man: (manufacturer || {}).id,
                old_man: processedText[1],
                new_model: (product || {}).id,
                old_model: processedText[2]
            });

        }
        catch (err) {
            console.log('err', err);
        }
    }
    connection.close();

    csvWriter
        .writeRecords(results)
        .then(()=> console.log('The CSV file was written successfully'));

})();
