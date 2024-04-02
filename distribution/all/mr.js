const crypto = require('crypto');
const { getNID } = require('../util/id.js');
const fs = require('fs');
const path = require('path');
const node = require('../local/node.js');

const mr = function (config) {
  // Generate a unique ID for each MapReduce invocation
  const jobId = crypto.randomUUID();
  let context = {};
  let nodeMap = {};
  let coordinatorNode = null
  let workerNodes = []
  let nodesToEndPointMap = {};
  context.gid = config.gid || 'all';

  console.log("bitch 1")



  function fetchNodeIDs(callback) {
    global.distribution.local.groups.get(context.gid, (e, v) => {
      if (e && Object.keys(e).length > 0) {
        return callback(new Error(e));
      } else {
        let allNodeInformation = Object.values(v);
        let nids = []; // Array to hold the node IDs

        allNodeInformation.forEach((node) => {
          let nodeID = getNID(node);
          nids.push(nodeID); // Add nodeID to the array
          nodeMap[nodeID] = node; // Map nodeID to the entire node object
        });

        // Return both the array of nodeIDs and the map via the callback
        return callback(null, nids);
      }
    });
  }


  function setupServiceEndpoints(callback) {
    let completedMapTasks = 0

    // service endpoint for the coordinator
    const coordinatorEndpoint = `mr-${jobId}-coordinator`;
    global.distribution.local[coordinatorEndpoint] = {
      notify: (message) => {
        // Receive notifications from worker nodes
        console.log(`Coordinator received notification: ${JSON.stringify(message)}`);
        // TODO: Implement logic to handle notifications and trigger next phases
        if (message.phase === 'map') {
          completedMapTasks++;
          if (completedMapTasks === workerNodes.length) {
            console.log('All nodes have finished mapping.');
            // TODO: Trigger the next phase (e.g., shuffle)
          }
        }
      },
      map: config.map,
      reduce: config.reduce,
    };

    // Create service endpoints for worker nodes
    const workerEndpoints = workerNodes.map((node, index) => `mr-${jobId}-worker-${index}`);
    workerEndpoints.forEach((endpoint, index) => {
      nodesToEndPointMap[endpoint] = workerNodes[index];
      global.distribution.local[endpoint] = {
        map: (jobId, config, keys) => {
          // Perform the map operation on each key-value pair
          keys.forEach((key) => {
            global.distribution.local.store.get(key, (e, value) => {
              if (e) {
                console.error(`Failed to get value for key ${key}:`, e);
                return;
              }
              const mappedResult = config.map(key, value);
              global.distribution.local.store.put(mappedResult, key, (e) => {
                if (e) {
                  console.error(`Failed to store mapped result for key ${key}:`, e);
                }
              });
            });
          });

          // Notify the coordinator that the map task is complete
          const coordinatorEndpoint = `mr-${jobId}-coordinator`;
          const remote = { node: coordinatorNode, service: coordinatorEndpoint, method: 'notify' };
          global.distribution.local.comm.send([{ phase: 'map' }], remote, (e) => {
            if (e) {
              console.error('Failed to notify coordinator:', e);
            }
          });
        },
        reduce: config.reduce,
      };
    });

    console.log("this is the nodemap ", nodesToEndPointMap)



    callback(null)
    // Store the endpoint identifiers for later cleanup
    // TODO: Implement a mechanism to store the endpoint identifiers for cleanup
  }

  function startMapPhase(keys) {
    console.log("bitch 3")
    const numWorkers = workerNodes.length;
    const keysPerWorker = Math.ceil(keys.length / numWorkers);

    for (let i = 0; i < numWorkers; i++) {
      const startIndex = i * keysPerWorker;
      const endIndex = Math.min(startIndex + keysPerWorker, keys.length);
      const workerKeys = keys.slice(startIndex, endIndex);

      const endpoint = `mr-${jobId}-worker-${i}`;
      const node = nodesToEndPointMap[endpoint];
      const remote = { node, service: endpoint, method: 'map' };
      global.distribution.local.comm.send([jobId, config, workerKeys], remote, (e) => {
        if (e) {
          console.log("we got an error");
        } else {
          console.log("please work");
        }
      });
    }

  }

  return {
    //config will contain the params (keys, map function, reduce function)
    exec: (configuration, callback) => {
      /* Change this with your own exciting Map Reduce code! */

      fetchNodeIDs((err, nodeIds) => {
        if (err) {
          console.error("Failed to fetch node IDs:", err);
          return;
        }

        // declare the coordinator and worker nodes
        coordinatorNode = nodeMap[nodeIds[0]]

        // remove the coordinator node from the list of nodeIds
        let nidsOfWorkerNodes = nodeIds.slice(1)

        workerNodes =
          nidsOfWorkerNodes.map(nid => nodeMap[nid]);

        // Setup service endpoints for the coordinator and worker nodes
        setupServiceEndpoints((err) => {
          if (err) {
            console.error("Failed to setup service endpoints:", err);
            return;
          }

          // Fetch the keys from the distributed storage
          global.distribution.local.store.get(null, (e, keys) => {
            if (e) {
              console.error("Failed to fetch keys:", e);
              return;
            }

            // Start the map phase with the fetched keys
            console.log("we are starting the map phase");
            startMapPhase(keys);
          });
        });
      });

      callback(null, []);
    },
  };
};

module.exports = mr;





// currently i think the problem is within the actual map function that i am passing into the worker node
// or the problem could be im not even passing the map function to the actual worker node.i know that im 
// creating the endpoint but is it connected with the actual worker node, im not super. i need to check all of this