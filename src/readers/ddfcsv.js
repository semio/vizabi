import * as utils from 'base/utils';
import Promise from 'base/promise';
import Reader from 'base/reader';


var DDFCSVReader = Reader.extend({

  /**
   * Initializes the reader.
   * @param {Object} reader_info Information about the reader
   */
  init: function (reader_info) {
    this._name = 'ddf-csv';
    this._data = [];
    this._ddfPath = reader_info.path;
  },

  /**
   * Reads from source
   * @param {Object} query to be performed
   * @param {String} language language
   * @returns a promise that will be resolved when data is read
   */
  read: function (queryPar, language) {
    // todo: add group by processing

    var _this = this;
    var query = utils.deepExtend({}, queryPar);

    _this.queryDescriptor = new QueryDescriptor(queryPar);

    if (_this.queryDescriptor.type === GEO) {
      return new Promise(function (resolve) {
        geoProcessing(_this._ddfPath, function () {
          _this._data = _this.getGeoData(_this.queryDescriptor);
          console.log('!GEO DATA', _this._data);
          resolve();
        });
      });
    }

    if (_this.queryDescriptor.type === MEASURES_TIME) {
      return new Promise(function (resolve) {
        getIndex(_this._ddfPath).then(function () {
          Promise
            .all(_this.getExpectedMeasures(query))
            .then(function () {
              var result = [];
              var geo = CACHE.DATA_CACHED['geo-' + _this.queryDescriptor.category];

              _this.queryDescriptor.timeRanges.forEach(function (time) {
                for (var geoIndex = 0; geoIndex < geo.length; geoIndex++) {
                  var line = {
                    'geo': geo[geoIndex].geo,
                    'time': new Date(time)
                  };

                  if (_this.injectMeasureValues(query, line, geoIndex, time) === true) {
                    result.push(line);
                  }
                }
              });

              _this._data = result;

              console.log('!QUERY', JSON.stringify(query));
              console.log('!OUT DATA', _this._data);
              console.log('!METADATA', Vizabi._globals.metadata);

              resolve();
            });
        });
      });
    }
  },

  /**
   * Gets the data
   * @returns all data
   */
  getData: function () {
    return this._data;
  },

  injectMeasureValues: function (query, line, geoIndex, time) {
    var f = 0;
    var measures = this.getMeasuresNames(query);
    var geo = CACHE.DATA_CACHED['geo-' + this.queryDescriptor.category];

    measures.forEach(function (m) {
      var measureCache = CACHE.FILE_CACHED[CACHE.measureFileToName[m]];

      if (measureCache && measureCache[geo[geoIndex].geo]) {
        if (measureCache[geo[geoIndex].geo] && measureCache[geo[geoIndex].geo][time] &&
          measureCache[geo[geoIndex].geo][time][m]) {
          line[m] = Number(measureCache[geo[geoIndex].geo][time][m]);
          f++;
        }
      }
    });

    if (query.select.indexOf('geo.latitude') > 0) {
      line['geo.latitude'] = geo[geoIndex]['geo.latitude'];
      ++f;
    }

    if (query.select.indexOf('geo.longitude') > 0) {
      line['geo.longitude'] = geo[geoIndex]['geo.longitude'];
      ++f;
    }

    return f === measures.length;
  },

  getGeoData: function (queryDescriptor) {
    var adapters = {
      country: function (geoRecord) {
        return {
          geo: geoRecord.geo,
          'geo.name': geoRecord.name,
          'geo.cat': queryDescriptor.category,
          'geo.region': geoRecord.world_4region,
          'geo.latitude': geoRecord.latitude,
          'geo.longitude': geoRecord.longitude
        }
      }
    };

    var expectedGeoData = null;
    for (var k in CACHE.FILE_CACHED) {
      if (CACHE.FILE_CACHED.hasOwnProperty(k) &&
        k.indexOf('ddf--list--geo--' + queryDescriptor.category) >= 0) {
        expectedGeoData = CACHE.FILE_CACHED[k];
        break;
      }
    }

    var result = [];
    if (expectedGeoData !== null) {
      expectedGeoData.forEach(function (d) {
        result.push(adapters[queryDescriptor.category](d));
      });
    }

    CACHE.DATA_CACHED['geo-' + queryDescriptor.category] = result;
    return result;
  },

  getMeasuresNames: function (query) {
    var res = [];
    query.select.forEach(function (q) {
      if (q !== 'time' && q !== 'geo') {
        res.push(q);
      }
    });

    return res;
  },

  getExpectedMeasures: function (query) {
    var _this = this;
    var expected = [];

    CACHE.FILE_CACHED[getIndexEntryPoint(_this._ddfPath)].forEach(function (indexRecord) {
      // todo: fix condition -> geo
      if (query.select.indexOf(indexRecord.value_concept) >= 0 &&
        (!query.where['geo.cat'] || query.where['geo.cat'].indexOf(indexRecord.geo) >= 0)) {
        var path = _this._ddfPath + '/' + indexRecord.file;
        // todo: swap...
        CACHE.measureFileToName[indexRecord.value_concept] = path;
        CACHE.measureNameToFile[path] = indexRecord.value_concept;
        expected.push(load(path));
      }
    });

    return expected;
  }
});

var GEO = 1;
var MEASURES_TIME = 2;

var CACHE = {
  measureFileToName: {},
  measureNameToFile: {},
  FILE_CACHED: {},
  FILE_REQUESTED: {},
  DATA_CACHED: {}
};

var EVALALLOWED = null;

function QueryDescriptor(query) {
  this.query = query;
  this.geoCat = query.where['geo.cat'];
  var result;

  if (query.select.indexOf('geo.name') >= 0 || query.select.indexOf('geo.region') >= 0) {
    this.type = GEO;
    this.category = this.geoCat[0];
  }

  if (!result && query.where && query.where.time) {
    this.type = MEASURES_TIME;
    this.category = this.geoCat[0];
    this.timeRanges = getRange(query.where.time);
  }
}

function geoProcessing(ddfPath, cb) {
  getDimensions(ddfPath).then(function () {
    Promise
      .all(getDimensionsDetails(ddfPath))
      .then(function () {
        cb();
      });
  });
}

function getDimensionsDetails(ddfPath) {
  var expected = [];
  var dimensionPath = getDimensionEntryPoint(ddfPath);

  CACHE.FILE_CACHED[dimensionPath].forEach(function (dimensionRecord) {
    if (dimensionRecord.concept !== 'geo' && dimensionRecord.concept !== 'un_state') {
      expected.push(load(ddfPath + '/ddf--list--geo--' + dimensionRecord.concept + '.csv'));
    }
  });

  return expected;
}

var load = function load(path, cb) {
  // checks if eval() statements are allowed. They are needed for fast parsing by D3.
  if (EVALALLOWED == null) {
    defineEvalAllowed();
  }

  // true:  load using csv, which uses d3.csv.parse, is faster but doesn't comply with CSP
  // false: load using text and d3.csv.parseRows to circumvent d3.csv.parse and comply with CSP
  var loader = (EVALALLOWED) ? d3.csv : d3.text;
  var parser = (EVALALLOWED) ? null : csvToObject;

  loader(path, function (error, res) {
    if (!res) {
      console.log('No permissions or empty file: ' + path, error);
    }

    if (error) {
      console.log('Error Happened While Loading CSV File: ' + path, error);
    }

    if (parser) {
      res = parser(res);
    }

    if (cb) {
      cb(error, res);
    }

    if (!cb) {
      // todo move measureHashTransformer ...
      CACHE.FILE_CACHED[path] = measureHashTransformer(CACHE.measureNameToFile[path], res);
      CACHE.FILE_REQUESTED[path].resolve();
    }
  });

  if (!cb) {
    CACHE.FILE_REQUESTED[path] = new Promise();
    return CACHE.FILE_REQUESTED[path];
  }
}

function defineEvalAllowed() {
  try {
    new Function("", "");
    EVALALLOWED = true;
  } catch (ignore) {
    // Content-Security-Policy does not allow "unsafe-eval".
    EVALALLOWED = false;
  }
}

// parsing csv string to an object, circumventing d3.parse which uses eval unsafe new Function() which doesn't comply with CSP
// https://developer.chrome.com/apps/contentSecurityPolicy
// https://github.com/mbostock/d3/pull/1910
function csvToObject(res) {
  var header;
  return (res == null) ? null : d3.csv.parseRows(res, function (row, i) {
    if (i) {
      var o = {}, j = -1, m = header.length;
      while (++j < m) o[header[j]] = row[j];
      return o;
    }
    header = row;
  });
}

function measureHashTransformer(measure, data) {
  if (!measure) {
    return data;
  }

  var hash = {};
  data.forEach(function (d) {
    if (!hash[d.geo]) {
      hash[d.geo] = {};
    }

    if (!hash[d.geo][d.year]) {
      hash[d.geo][d.year] = {};
    }

    hash[d.geo][d.year][measure] = d[measure];
  });

  return hash;
}

function getIndexEntryPoint(ddfUrl) {
  return ddfUrl + '/ddf--index.csv';
}

function getIndex(ddfUrl) {
  return load(getIndexEntryPoint(ddfUrl));
}

function getMeasuresEntryPoint(ddfUrl) {
  return ddfUrl + '/ddf--measures.csv';
}

function getMeasures(ddfUrl, cb) {
  return load(getMeasuresEntryPoint(ddfUrl), cb);
}

function getDimensionEntryPoint(ddfPath) {
  return ddfPath + '/ddf--dimensions.csv';
}

function getDimensions(ddfPath, cb) {
  return load(getDimensionEntryPoint(ddfPath), cb);
}



//// time utils

function flatten(arr) {
  return arr.reduce(function (prev, cur) {
    var more = [].concat(cur).some(Array.isArray);
    return prev.concat(more ? cur.flatten() : cur);
  }, []);
}

function getUnique(arr) {
  var u = {};
  var a = [];
  for (var i = 0, l = arr.length; i < l; ++i) {
    if (u.hasOwnProperty(arr[i])) {
      continue;
    }

    a.push(arr[i]);
    u[arr[i]] = 1;
  }
  return a;
}

var TYPE_PATTERN = [
  // year
  /^(\d{4})$/,
  // quarter
  /^(\d{4})q(\d{1})$/,
  // month
  /^(\d{4})(\d{2})$/,
  // week
  /^(\d{4})w(\d{1,2})$/,
  // date
  /^(\d{4})(\d{2})(\d{2})$/
];

function extractLocalRange(type) {
  function parse(option) {
    var match1 = TYPE_PATTERN[type].exec(option[0]);
    var match2 = TYPE_PATTERN[type].exec(option[1]);

    return {
      first: [match1[1], match1[2], match1[3]],
      second: [match2[1], match2[2], match2[3]]
    };
  }

  function getTypicalRange(option, minLimit, maxLimit, divider, isFullV) {
    var parsed = parse(option);
    var sYear = Number(parsed.first[0]);
    var v1 = Number(parsed.first[1]);
    var fYear = Number(parsed.second[0]);
    var v2 = Number(parsed.second[1]);

    var result = [];
    for (var year = sYear; year <= fYear; year++) {
      var sV = year === sYear ? v1 : minLimit;
      var fV = year === fYear ? v2 : maxLimit;
      for (var v = sV; v <= fV; v++) {
        if (isFullV === true && v < 10) {
          v = '0' + v;
        }

        result.push(year + divider + v);
      }
    }

    return result;
  }

  var options = [
    function year(option) {
      var parsed = parse(option);
      var sYear = Number(parsed.first[0]);
      var fYear = Number(parsed.second[0]);

      var result = [];
      for (var year = sYear; year <= fYear; year++) {
        result.push('' + year);
      }

      return result;
    },
    function quarter(option) {
      return getTypicalRange(option, 1, 4, 'q', false);
    },
    function month(option) {
      return getTypicalRange(option, 1, 12, '', true);
    },
    function week(option) {
      return getTypicalRange(option, 1, 53, 'w', true);
    },
    function date(option) {
      var parsed = parse(option);
      var sYear = Number(parsed.first[0]);
      var month1 = Number(parsed.first[1]);
      var day1 = Number(parsed.first[2]);
      var fYear = Number(parsed.second[0]);
      var month2 = Number(parsed.second[1]);
      var day2 = Number(parsed.second[2]);

      var result = [];
      for (var year = sYear; year <= fYear; year++) {
        var sMonth = year === sYear ? month1 : 1;
        var fMonth = year === fYear ? month2 : 12;
        for (var month = sMonth; month <= fMonth; month++) {
          var monthStr = month < 10 ? '0' + month : month;
          var sDay = (year === sYear && month === sMonth) ? day1 : 1;
          var fDay = (year === fYear && month === fMonth) ? day2 : 31;

          for (var day = sDay; day <= fDay; day++) {
            var dayStr = day < 10 ? '0' + day : day;

            result.push(year + '' + monthStr + '' + dayStr);
          }
        }
      }

      return result;
    }
  ];

  return options[type];
}

function detectType(timeQuery) {
  var flat = flatten(timeQuery);
  var types = [];
  for (var i = 0; i < flat.length; i++) {
    for (var j = 0; j < TYPE_PATTERN.length; j++) {
      if (TYPE_PATTERN[j].test(flat[i])) {
        types.push(j);
        break;
      }
    }
  }

  types = getUnique(types);

  if (types.length !== 1) {
    throw new Error('Wrong time query format: ' + JSON.stringify(timeQuery));
  }

  return types[0];
}


var getRange = function getRange(query) {
  var type = detectType(query);

  var extractor = extractLocalRange(type);
  var result = [];
  query.forEach(function (option) {
    if (typeof option === 'string') {
      result.push(option);
    }

    if (typeof option === 'object') {
      result = result.concat(extractor(option));
    }
  });

  return result;
};



export default DDFCSVReader;