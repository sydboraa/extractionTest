const { NGrams, RegexpTokenizer } = require('natural');
const mysql = require('mysql2/promise');
const squel = require('squel');
const _ = require('lodash');
const { Maybe } = require('monet');
const csv = require('async-csv');
const fs = require('fs').promises;
const { createObjectCsvWriter } = require('csv-writer');

const csvWriter = createObjectCsvWriter({
    path: 'natural-results.csv',
    header: [
        {id: 'title', title: 'title'},
        {id: 'new_man', title: 'new_man'},
        {id: 'old_man', title: 'old_man'},
        {id: 'new_model', title: 'new_model'},
        {id: 'old_model', title: 'old_model'}
    ]
});
const specialChars = ['-', '/', '.'];

// const [, , ...text] = process.argv;
// const term = text.join(' ');
const tokenizer = new RegexpTokenizer({ pattern: /\s+/ });

const replaceAll = (str, find, replace) => str.split(find).join(replace);
const replaceSpecialChars = str => specialChars.reduce((acc, toReplace, i, chars) => {
    return [...acc, ...chars.map(replaceWith => replaceAll(str, toReplace, replaceWith))];
}, []);

const withSpecialChars = (tokens) => {
    const replaced = [...tokens];
    tokens.forEach(t => replaced.push(...replaceSpecialChars(t)));

    return [...new Set(replaced).values()];
};

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

    if(_.isEmpty(manufacturerIds)) { // added empty check
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

(async () => {
    const connection = await mysql.createConnection({
       //TODO
    });
    const results = [];

    const csvString = await fs.readFile('./samples.csv', 'utf-8');

    const rows = await csv.parse(csvString);

    for(let i=1; i<rows.length; i++){
        const searches = [];

        // normalize: handle 'bp 20/50' by creating alternatives 'bp 20-50' etc or vice versa

        const processedText = rows[i]; //term
        const tokens = tokenizer.tokenize(processedText[0].toLocaleLowerCase());

        // we iterate downwards so that matches with MORE words will have higher priority
        for (const size of _.rangeRight(1, 6)) {
            searches.push({
                ngrams: withSpecialChars(NGrams.ngrams(tokens, size).map(x => x.join(' '))),
                size
            });
        }

        const filtered = searches.filter(x=>!_.isEmpty(x.ngrams)); //added empty check
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

    connection.close();

    csvWriter
        .writeRecords(results)
        .then(()=> console.log('The CSV file was written successfully'));

})();
