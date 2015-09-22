//service wraps single endpoint

angular.module('gapminderTools')
  .factory("vizabiFactory", ['$rootScope', '$timeout', function ($rootScope, $timeout) {

    return {

      /**
       * Render Vizabi
       * @param {String} tool name of the tool
       * @param {DOMElement} placeholder
       * @return {Object}
       */
      render: function (tool, placeholder, options) {

        var hash = window.location.hash;
        if (hash) {
          var state = JSON.parse(location.hash.substr(1), function (key, value) {
              if (key == 'value' || key == 'end' || key == 'start') {
                var date = new Date(value);
                return date.getFullYear().toString();
              }
              return value;
            }
          );

          options.language = {};
          options.language.id = state.id;
          options.state = state.state;
        }

        return Vizabi(tool, placeholder, options);
      }
    };

  }]);


angular.module('gapminderTools')
  .factory("vizabiItems", ['$http', function ($http) {

    return {
      /**
       * Get All Items
       */
      getItems: function () {
        //return the promise directly.
        return $http.get(baseHref + 'api/item')
          .then(function (result) {
            var items = {}, i, s;
            for (i = 0, s = result.data.length; i < s; i++) {
              items[result.data[i].slug] = result.data[i];
            }
            return items;
          });
      }
    };

  }]);

angular.module('gapminderTools')
  .factory('menuFactory', ['$location', '$q', '$http', function ($location, $q, $http) {
    return {
      cached: [],

      /**
       * Get All Items
       */
      getMenu: function () {
        //return the promise directly.
        var _this = this;
        return $http.get(baseHref + 'api/menu')
          .then(function (result) {
            if (result.status === 200) {
              _this.cached = result.data.children;
            }
            return _this.getCachedMenu();
          });
      },

      /**
       * Returns the home tree data.
       * @returns {}
       */
      getCachedMenu: function () {
        return this.cached;
      },

      /**
       * Returns the current URL.
       * @returns {string}
       */
      getCurrentUrl: function () {
        return $location.$$path;
      }
    };
  }]);

angular.module('gapminderTools')
  .factory("vizabiIndicators", ['$http', '$log', function ($http, $log) {

    return {
      /**
       * Get All Indicators
       */
      getIndicators: function (cb) {
        //return the promise directly.
        return $http.get(baseHref + 'api/indicators/stub')
          .then(function (res) {
            var rows = res.data.data.rows;
            var headers = res.data.data.headers;
            var result = new Array(rows.length);
            // unwrap compact data into json collection
            for (var i = 0; i < rows.length; i++) {
              result[i] = {};
              for (var j = 0; j < headers.length; j++) {
                result[i][headers[j]] = (rows[i][j] || '').toString();
              }
            }
            return cb(res.data.error, result);
          }, function (response) {
            $log.error(response);
            return cb(response);
          });
      }
    };

  }]);
