const { Router } = require('express');
const ctrl = require('../controllers/search.controller');
const router = Router();

//WE NEED TO IMPLEMENT 2 MAJOR FUNCTIONALITY IN SEARCH SERVICE
                      //1->FUZZY SEARCHING-->even if user puts wrong spelling then to give result based on neares match
                      //2->AUTO COMPLETION

//YE DONO FETURES ARE PROVIDED BY ELASTIC SEARCH AND pg_tgram(EXTENTION OF POSTGRES) BUT WE WILL USE ELASTIC SEARCH COZ ITS FASTER
//Elastic search me we store data in json format as opposed to data that we store in sql db's like mySql
           /*example-->{
                           "trainId": "123",
                           "trainName": "abc",
                           "stations": [a,b,c,d,e,f]
                       }*/

//WHY IS SEARCHING FASTER IN ELASTIC SEARCH-->oz elastic search stores data in a INVERTED INDEX data structure jiski wjah se search queries faser ho jai hai
//see 01.png-->lets say our data is stored in table form like relational db now if we want o find ki france kis kis document me present hai then ek ek krkr har row pr iterate krna hoga and laency will increase as the number of ros in the dataset increase
//see 02.png--->in elastic search we store data like this to ek saath hi patha chal gya ki france kin kin docId me hai

//INDEX->group of json objects(similar to tables in relational db)
//DOCUMENTS->json message
//INDEX TEMPLATE->blueprint jiski help se hmm index create kr skte hai-->ye batata hai ki hame index create krte smay kon kon si settings(data mapping(defines schema of documents and are of two types static and dynamic data mapping),priority of index) apply karni hai


//see 04.png-->to get the idea of architecture
               //admin service in architecture is responsible for adding new train/route/stations/schedule
//see 05.png-->jb bhi user kisi bhi 2 station ke beech ki available trains  find krne ki req krega then that req will be fulfilled by search service(elastic search) but elastic search will not be the source of truth for info like train kis data/time pr chal rhi hai,abhi running status kya hai,tain me seats kitni hai to in saari info ke lie we use inventory service
// GET /search/trains?from=Delhi&to=Mumbai&date=2025-07-15
router.get('/trains', ctrl.searchTrains);

// GET /search/autocomplete?q=del
router.get('/autocomplete', ctrl.autocomplete);

// Debug endpoints
router.get('/debug/stations', ctrl.debugStations);
router.get('/debug/trains', ctrl.debugTrains);

module.exports = router;