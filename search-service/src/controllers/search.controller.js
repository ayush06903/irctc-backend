//yha pr we are actualluy implementing how the user will use search functionality 
const asyncHandler = require('../utils/asyncHandler');
const { BadRequestError } = require('../utils/error');
const searchService = require('../services/search.service');

exports.searchTrains = asyncHandler(async (req, res) => {
     const { from, to, date } = req.query;        //ye hmne req.query se to,from destination and date fetch krli
     if (!from || !to) throw new BadRequestError('from and to station names/codes are required');
     const results = await searchService.searchTrains(from, to, date || null);
                                   //searchTrains method ka business logic searchService.js me hai
     res.json({ success: true, data: results });
});

//implementing auto complete feature
exports.autocomplete = asyncHandler(async (req, res) => {
     const { q } = req.query;
     if (!q || q.length < 2) throw new BadRequestError('Provide at least 2 characters');
     const suggestions = await searchService.autocompleteStation(q);
     res.json({ success: true, data: suggestions });
});

exports.debugStations = asyncHandler(async (req, res) => {
     const data = await searchService.getAllStations();
     res.json({ success: true, count: data.length, data });
});

exports.debugTrains = asyncHandler(async (req, res) => {
     const data = await searchService.getAllTrains();
     res.json({ success: true, count: data.length, data });
});