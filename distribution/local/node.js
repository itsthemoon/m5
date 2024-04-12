const http = require('http');
const url = require('url');
const crypto = require('crypto');

let local = require('../local/local');
const serialization = require('../util/serialization');
const mrEndpoints = new Map();


/*
    The start function will be called to start your node.
    It will take a callback as an argument.
    After your node has booted, you should call the callback.
*/


function isValidBody(body) {
  error = undefined;
  if (body.length === 0) {
    return new Error('No body');
  }

  try {
    body = JSON.parse(body);
  } catch (error) {
    return error;
  }

  return error;
}


const start = function(onStart) {
  const server = http.createServer((req, res) => {
    /* Your server will be listening for PUT requests. */

    // Write some code...

    if (req.method !== 'PUT') {
      res.end(serialization.serialize(new Error('Method not allowed!')));
      return;
    }

    /*
      The path of the http request will determine the service to be used.
      The url will have the form: http://node_ip:node_port/service/method
    */


    // Write some code...


    const pathname = url.parse(req.url).pathname;
    const [, service, method] = pathname.split('/');

    // console.log(`[SERVER] (${global.nodeConfig.ip}:${global.nodeConfig.port})
    //     Request: ${service}:${method}`);


    /*

      A common pattern in handling HTTP requests in Node.js is to have a
      subroutine that collects all the data chunks belonging to the same
      request. These chunks are aggregated into a body variable.

      When the req.on('end') event is emitted, it signifies that all data from
      the request has been received. Typically, this data is in the form of a
      string. To work with this data in a structured format, it is often parsed
      into a JSON object using JSON.parse(body), provided the data is in JSON
      format.

      Our nodes expect data in JSON format.
  */

    // Write some code...


    let body = [];

    req.on('data', (chunk) => {
      body.push(chunk);
    });

    req.on('end', () => {
      body = Buffer.concat(body).toString();

      let error;

      if (error = isValidBody(body)) {
        res.end(serialization.serialize(error));
        return;
      }

      body = JSON.parse(body);
      body = serialization.deserialize(body);
      let args = body;


      /* Here, you can handle the service requests. */

      // Write some code...

      // Check if the service is 'mr'
      if (service === 'mr' || service.startsWith('mr-')) {
        if (method === 'registerEndpoint') {
          // Register the endpoint
          const [endpoint, notify] = args;
          mrEndpoints.set(endpoint, {notify: notify});

          res.end(serialization.serialize([null, 'Success']));
        } else if (method === 'registerFunctions') {
          // Register the map and reduce functions
          const [jobId, map, reduce, shuffle, shuffleData,
            trueMap, trueReduce, trueCompact] = args;
          const endpoint = `mr-${jobId}`;
          const mrEndpoint = mrEndpoints.get(endpoint);
          if (mrEndpoint) {
            mrEndpoint.map = map;
            mrEndpoint.reduce = reduce;
            mrEndpoint.shuffle = shuffle;
            mrEndpoint.shuffleData = shuffleData;
            mrEndpoint.trueMap = trueMap;
            mrEndpoint.trueReduce = trueReduce;
            mrEndpoint.trueCompact = trueCompact;
            res.end(serialization.serialize([null, 'Success']));
          } else {
            res.end(serialization.serialize([new
            Error('MapReduce endpoint not found'), null]));
          }
        } else {
          // Handle the other methods
          const mrEndpoint = mrEndpoints.get(service);
          if ((mrEndpoint && mrEndpoint[method]) ||
            (method === 'deregisterEndpoint')) {
            if (!mrEndpoint) {
              res.end(serialization.serialize([null, 'Already deregistered']));
              return;
            }
            const {map, trueMap, reduce, shuffle,
              shuffleData, trueReduce, notify, trueCompact} = mrEndpoint;

            if (method === 'map') {
              const [workerKeys, gid, jobId] = args;
              notify(map, workerKeys, gid, jobId, trueMap, 'map',
                  trueCompact, (e, v) => {
                    res.end(serialization.serialize([e, v]));
                  });
            } else if (method === 'reduce') {
              const [gid, jobId] = args;
              notify(reduce, null, gid, jobId, trueReduce, 'reduce',
                  null, (e, v) => {
                    res.end(serialization.serialize([e, v]));
                  });
            } else if (method === 'deregisterEndpoint') {
              const [jobId] = args;
              const endpoint = `mr-${jobId}`;
              mrEndpoints.delete(endpoint);
              res.end(serialization.serialize([null, 'Success']));
            } else if (method === 'shuffle') {
              // this method is called shuffleFunction in mr.js
              // we are passing some null values as placeholders
              const [jobId, gid] = args;
              shuffle(crypto, gid, jobId, (e, v) => {
                res.end(serialization.serialize([e, v]));
              });
            } else if (method === 'shuffleData') {
              const [key, keyValuePair, gid, jobId] = args;
              shuffleData(key, keyValuePair, jobId, gid, (e, v) => {
                res.end(serialization.serialize([e, v]));
              });
            }
          } else {
            res.end(serialization.serialize(
                [new Error('MapReduce method not found'), null]));
          }
        }
      } else {
        local.routes.get(service, (error, service) => {
          if (error) {
            res.end(serialization.serialize(error));
            console.error(error);
            return;
          }

          /*
        Here, we provide a default callback which will be passed to services.
        It will be called by the service with the result of it's call
        then it will serialize the result and send it back to the caller.
          */
          const serviceCallback = (e, v) => {
            res.end(serialization.serialize([e, v]));
          };

          // Write some code...


          // console.log(`[SERVER] Args: ${JSON.stringify(args)}
          //   ServiceCallback: ${serviceCallback}`);

          service[method](...args, serviceCallback);
        });
      }
    });
  });


  // Write some code...

  /*
    Your server will be listening on the port and ip specified in the config
    You'll be calling the onStart callback when your server has successfully
    started.

    In this milestone, we'll be adding the ability to stop a node
    remotely through the service interface.
  */

  server.listen(global.nodeConfig.port, global.nodeConfig.ip, () => {
    console.log(`Server running at http://${global.nodeConfig.ip}:${global.nodeConfig.port}/`);
    onStart(server);
  });
};

module.exports = {
  start: start,
};
