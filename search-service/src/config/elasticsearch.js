//we know ki jaise sql db me tables hoti hai vaise hi elastic search me indexes hote hain aur un indexes me hmm json forma me data store krte hain
const { Client } = require('@elastic/elasticsearch');
const { config } = require('.');
const logger = require('./logger');

const esClient = new Client({ node: config.ELASTICSEARCH_URL });             //created elastic search client which will allow node js to talk with elastic search

//jin indexes ki hmne definations me baat kri thi un indexes ko hmne create kara
const STATION_INDEX = 'stations';
const TRAIN_INDEX = 'trains';
const ROUTE_INDEX = 'routes';
const SCHEDULE_INDEX = 'schedules';

/**
 * Create indices with mappings that power:
 *   • Fuzzy search  (match + fuzziness)
 *   • Autocomplete  (completion suggester)
 *   • Full-text     (text analyzer)
 */
    //this is the actual method which will create indices of sations
const initIndices = async () => {
     // ── Station index (for autocomplete) ──
     const stationExists = await esClient.indices.exists({ index: STATION_INDEX });
     if (!stationExists) {              //if this STATION_INDEX is already not present then we will create new index
          await esClient.indices.create({
               index: STATION_INDEX,
               //jb bhi hmm naya index create krte hai to hmm us index ke lie we define 2 things
                             //1->settings---->batata hai ki jo elastic search hai vo bheje gye text ko kaise process krega
                             //2->mappings
               settings: {
                    //yha neeche analysis aur analyzer dono text processing pipeline hain
                    //they perform operations like dividing text into diffrent tokens,lower casing the whole text
                                 //for ex->if we give new delhi in text processing pipeline then it will create 2 tokens of it 1->new
                                                                                                                             //2->delhi--->did so based on space
                    analysis: {
                         analyzer: {
                              //ye autocomplete analyzer provides us autocomplete feature
                                                 //ex->if user types "de" then it automatically suggests "delhi" to user
                              autocomplete_analyzer: {
                                   type: 'custom',
                                   tokenizer: 'autocomplete_tokenizer',
                                   filter: ['lowercase'],       //iski wajaah se jo searching hogi vo case sensitive hogi doesnt matter useer ne upper case me city ka naam likha ho ya lower case me
                              },
                              //we have 1->index analyzer-->iska use krenge jb hmm elastic search me data store krenge
                                                          //this simply jo text hai usko tokens me divide krega and then un tokens ke prefix create krega for example "delhi" ke prefix hain "d","de","del","delh" aut then it will store these prefixes into elastic search

                                      //2->search analyzer--?is search analyzer ka use hmm tb krenge jb hmm searching perform krenge..... to elastic search match krega user ki wuery ko stored tokens se for ex->if user provides del and they are still typing then elasic search jo hmare stored prefixes hain unse is ko match krenge and if it does match then it will suggest those text
                              search_analyzer: {
                                   type: 'custom',
                                   tokenizer: 'standard',
                                   filter: ['lowercase'],
                              },
                         },
                         tokenizer: {
                              autocomplete_tokenizer: {
                                   type: 'edge_ngram',
                                   min_gram: 2,
                                   max_gram: 20,
                                   token_chars: ['letter', 'digit'],
                              },
                         },
                    },
               },
               //mapping is basically schema of jo bhi indexes hmm create krenge
               mappings: {
                    properties: {
                         stationId: { type: 'keyword' },            //jin fields me hme exact match chahiye unme we put keyword
                         name: { type: 'text', analyzer: 'autocomplete_analyzer', search_analyzer: 'search_analyzer' },
                         code: { type: 'keyword' },
                         city: { type: 'text', analyzer: 'autocomplete_analyzer', search_analyzer: 'search_analyzer' },
                         suggest: { type: 'completion' },
                    },
               },
          });
          logger.info('Station index created');
     }

     // ── Train index ──
     const trainExists = await esClient.indices.exists({ index: TRAIN_INDEX });
     if (!trainExists) {
          await esClient.indices.create({
               index: TRAIN_INDEX,
               mappings: {
                    properties: {
                         trainId: { type: 'keyword' },
                         trainNumber: { type: 'keyword' },
                         trainName: { type: 'text' },
                         route: {            //this means ki hmm TRAIN_INDEX me route ka data bhi store krenge
                              type: 'nested',
                              properties: {
                                   stationId: { type: 'keyword' },
                                   stationName: { type: 'text' },
                                   stationCode: { type: 'keyword' },
                                   sequenceNumber: { type: 'integer' },
                                   arrivalTime: { type: 'keyword' },
                                   departureTime: { type: 'keyword' },
                                   distanceFromOrigin: { type: 'float' },
                              },
                         },
                         schedules: {           //TRAIN_INDEX will also include data present in scheddules model/table
                              type: 'nested',
                              properties: {
                                   scheduleId: { type: 'keyword' },
                                   departureDate: { type: 'date' },
                                   status: { type: 'keyword' },
                                   available: { type: 'integer' },
                                   locked: { type: 'integer' },
                                   booked: { type: 'integer' },
                              },
                         },
                         seatSummary: {           //will also include data realted to seat
                              properties: {
                                   total: { type: 'integer' },
                                   LOWER: { type: 'integer' },
                                   MIDDLE: { type: 'integer' },
                                   UPPER: { type: 'integer' },
                                   SIDE_LOWER: { type: 'integer' },
                                   SIDE_UPPER: { type: 'integer' },
                              },
                         },
                    },
               },
          });
          logger.info('Train index created');
     }
};

const recreateIndices = async () => {
     for (const index of [STATION_INDEX, TRAIN_INDEX]) {
          const exists = await esClient.indices.exists({ index });
          if (exists) {
               await esClient.indices.delete({ index });
               logger.info(`Deleted index: ${index}`);
          }
     }
     await initIndices();
     logger.info('All indices recreated');
};

module.exports = { esClient, STATION_INDEX, TRAIN_INDEX, ROUTE_INDEX, SCHEDULE_INDEX, initIndices, recreateIndices };