const crypto = require('crypto');
const {getNID, consistentHash} = require('../util/id.js');

const mr = function(config) {
  const jobId = crypto.randomUUID();
  let context = {};
  let nodeMap = {};
  let workerNodes = [];
  context.gid = config.gid || 'all';
  const keyDistribution = {};
  const nodeDetailToNodeIdMap = {};

  function fetchNodes(callback) {
    global.distribution.local.groups.get(context.gid, (e, v) => {
      if (e && Object.keys(e).length > 0) {
        return callback(new Error(e));
      } else {
        nodeMap = v;
        return callback(null, Object.keys(nodeMap));
      }
    });
  }

  function sendEndpointToAllNodes(endpoint, allNodes, notify) {
    allNodes.forEach((node) => {
      const remote = {node, service: 'mr', method: 'registerEndpoint'};
      global.distribution[context.gid].comm.send([endpoint, notify], remote,
          (e, v) => {
            if (e && Object.keys(e).length > 0) {
              console.error(`Failed to send endpoint to node ${node}:`, e);
            } else {
              console.log('we successfully sent endpoint to ', node);
            }
          });
    });
  }

  function sendFunctionsToAllNodes(map, reduce, shuffle, shuffleData, trueMap,
      trueReduce, trueCompact) {
    const allNodes = Object.values(nodeMap);
    allNodes.forEach((node) => {
      const remote = {
        node, service: `mr-${jobId}`,
        method: 'registerFunctions',
      };
      global.distribution[context.gid].comm.send(
          [jobId, map, reduce, shuffle,
            shuffleData, trueMap, trueReduce, trueCompact], remote, (e, v) => {
            if (e && Object.keys(e).length > 0) {
              console.error(`Failed to send functions to node ${node}:`, e);
            } else {
              console.log('we successfully sent functions to ', node);
            }
          });
    });
  }

  function deregisterEndpoint() {
    const allNodes = Object.values(nodeMap);
    allNodes.forEach((node) => {
      const remote = {
        node: node, service: `mr-${jobId}`,
        method: 'deregisterEndpoint',
      };
      global.distribution[context.gid].comm.send([jobId],
          remote, (e, v) => {
            if (e && Object.keys(e).length > 0) {
              console.error(`Failed to deregister job from node ${node}:`, e);
            }
          });
    });
  }

  const map = (callback) => {
    const promises = workerNodes.map((node) => {
      const nodeId = nodeDetailToNodeIdMap[`${node.ip}:${node.port}`];
      const workerKeys = keyDistribution[nodeId];
      const remote = {node, service: `mr-${jobId}`, method: 'map'};
      return new Promise((resolve, reject) => {
        global.distribution[context.gid].comm.send([workerKeys,
          context.gid, jobId], remote, (e, v) => {
          if (e && Object.keys(e).length > 0) {
            console.error('Failed to send map task to worker:',
                e, 'worker keys ',
                workerKeys, 'context.gid ', context.gid,
                'worker key distribution ',
                keyDistribution);
            reject(e);
          } else {
            resolve(v);
          }
        });
      });
    });

    Promise.all(promises)
        .then((mapResults) => {
          console.log('Map phase completed ', mapResults);
          callback(null, mapResults);
        })
        .catch((error) => {
          console.error('Map phase failed:', error);
          callback(error, null);
        });
  };

  const shuffle = (callback) => {
    const promises = workerNodes.map((node) => {
      return new Promise((resolve, reject) => {
        const remote = {node, service: `mr-${jobId}`, method: 'shuffle'};
        global.distribution[context.gid].comm.send([jobId, context.gid], remote,
            (e, v) => {
              if (e && Object.keys(e).length > 0) {
                console.error('Failed to initiate shuffle on worker node:',
                    e, v);
                reject(e);
              } else {
                resolve();
              }
            });
      });
    });

    Promise.all(promises)
        .then(() => {
          console.log('Shuffle phase completed');
          callback();
        })
        .catch((error) => {
          console.error('Shuffle phase failed:', error);
          callback(error);
        });
  };

  const reduce = (callback) => {
    const promises = workerNodes.map((node) => {
      const remote = {node, service: `mr-${jobId}`, method: 'reduce'};
      return new Promise((resolve, reject) => {
        global.distribution[context.gid].comm.send([context.gid, jobId],
            remote,
            (e, v) => {
              if (e && Object.keys(e).length > 0) {
                console.error('Failed to send reduce task to worker:', e, v);
                reject(e);
              } else {
                resolve([v]);
              }
            });
      });
    });

    Promise.all(promises)
        .then((reduceResults) => {
          console.log('Reduce phase completed');
          callback(null, reduceResults);
        })
        .catch((error) => {
          console.error('Reduce phase failed:', error);
          callback(error, null);
        });
  };

  return {
    exec: (configuration, callback) => {
      fetchNodes((err, nodeIds) => {
        if (err) {
          console.error('Failed to fetch node IDs:', err);
          return;
        }

        const endpoint = `mr-${jobId}`;
        workerNodes = nodeIds.map((nid) => nodeMap[nid]);
        workerNodes.forEach((node, index) => {
          const nodeIdentifier = `${node.ip}:${node.port}`;
          nodeDetailToNodeIdMap[nodeIdentifier] = nodeIds[index];
        });
        const workerNodeNIDs = workerNodes.map(getNID);
        const keys = configuration.keys;

        const nidToNodeIdMap = Object.fromEntries(
            workerNodes.map((node, index) => [getNID(node), nodeIds[index]]),
        );

        nodeIds.forEach((nodeId) => {
          keyDistribution[nodeId] = [];
        });

        keys.forEach((key) => {
          const hashedKey = crypto.createHash('sha256').update(key).
              digest('hex');
          const nid = consistentHash(hashedKey, workerNodeNIDs);
          const nodeId = nidToNodeIdMap[nid];
          keyDistribution[nodeId].push(key);
        });

        // Notify function that is passed to the workers
        const notify = (funct, keys, gid, jobId, trueFunc, typeOfFunc,
            helperFunction, callback) => {
          if (typeOfFunc === 'map') {
            funct(keys, gid, jobId, trueFunc, helperFunction, (e, v) => {
              if (error && Object.keys(error).length > 0) {
                console.log(`Error executing notify function:`, e, v);
                callback(e, null);
              } else {
                callback(null, v);
              }
            });
          } else {
            funct(keys, gid, jobId, trueFunc, (e, v) => {
              if (error && Object.keys(error).length > 0) {
                console.log(`Error executing notify function:`, e, v);
                callback(e, null);
              } else {
                callback(null, v);
              }
            });
          }
        };

        const mapFunction = (keys, gid, jobId, trueMap,
            trueCompact, callback) => {
          const result = {};

          if (keys.length === 0) {
            callback(null, result);
            return;
          }

          let completedCount = 0;

          global.distribution[gid].store.get(null, (e, v) => {
            if (e && Object.keys(e).length > 0) {
              console.error('Failed to fetch data from local storage:', e);
              callback(e, null);
              return;
            } else {
              const totalCount = keys.length;

              const saveResults = (keys, callback) => {
                const promises = keys.map((key) => {
                  return new Promise((resolve, reject) => {
                    global.distribution[gid].store.put(
                        JSON.stringify(result[key]),
                        `map_output_${key}_${jobId}`, (e) => {
                          if (e && Object.keys(e).length > 0) {
                            console.error(
                                'Failed to save result to local storage:', e);
                            reject(e);
                          } else {
                            resolve();
                          }
                        });
                  });
                });

                Promise.all(promises)
                    .then(() => {
                      console.log('All results saved');
                      callback(null);
                    })
                    .catch((error) => {
                      console.error('Failed to save results:', error);
                      callback(error);
                    });
              };

              keys.forEach((key) => {
                if (v.includes(key)) {
                  global.distribution[gid].store.get(key,
                      async (e, v) => {
                        if (e && Object.keys(e).length > 0) {
                          console.error(
                              'Failed to fetch data from local storage:', e);
                          callback(e, null);
                          return;
                        } else {
                          console.log('the value for map ', v);
                          const tempResult = await trueMap(key, v);
                          console.log('the temp result ', tempResult);
                          const compactedResult =
                        trueCompact ? trueCompact(tempResult) : tempResult;
                          result[key] = compactedResult;
                          completedCount++;

                          if (completedCount === totalCount) {
                            saveResults(Object.keys(result), (e) => {
                              if (e) {
                                console.error(
                                    'Failed to save map results:', e);
                                callback(e, null);
                                return;
                              }
                              console.log('Map results saved successfully ',
                                  result);
                              callback(null, result);
                            });
                          }
                        }
                      });
                } else {
                  completedCount++;

                  if (completedCount === totalCount) {
                    saveResults(Object.keys(result), (e) => {
                      if (e) {
                        console.error(
                            'Failed to save map results:', e);
                        callback(e, null);
                        return;
                      }

                      callback(null, result);
                    });
                  }
                }
              });
            }
          });
        };

        const shuffleFunction = (crypto, gid, jobId, callback) => {
          let completedCount = 0;

          global.distribution[gid].store.get(null, (e, v) => {
            if (e && Object.keys(e).length > 0) {
              console.error(
                  'Failed to fetch data from local storage:', e);
              callback(e);
              return;
            }

            const mapOutputKeys = v.filter((key) =>
              key.startsWith(`map_output_`) && key.endsWith(`_${jobId}`));
            const totalCount = mapOutputKeys.length;

            mapOutputKeys.forEach((key) => {
              global.distribution[gid].store.get(key, (e, value) => {
                if (e && Object.keys(e).length > 0) {
                  console.error(
                      'Failed to fetch map output from local storage:', e);
                  callback(e);
                  return;
                }

                const keyValuePair = JSON.parse(value);
                console.log('the key value pair ', keyValuePair);
                let nodeMap = {};

                // workerNodes
                global.distribution.local.groups.get(gid, (e, v) => {
                  if (e && Object.keys(e).length > 0) {
                    return callback(new Error(e));
                  } else {
                    nodeMap = v;
                    const workerNodes = Object.values(nodeMap);
                    const hash = crypto.createHash('sha256').
                        update(key).digest('hex');
                    const hashValue = parseInt(hash.substring(0, 8), 16);
                    const targetNodeIndex = hashValue % workerNodes.length;
                    const targetNode = workerNodes[targetNodeIndex];

                    // send the shuffle data to the target node
                    const remote = {
                      targetNode, service: `mr-${jobId}`,
                      method: 'shuffleData',
                    };
                    global.distribution[gid].comm.send([key, keyValuePair,
                      gid, jobId], remote, (e) => {
                      if (e && Object.keys(e).length > 0) {
                        console.error(
                            'Failed to send shuffle data to target node:', e);
                        callback(e);
                        return;
                      }

                      completedCount++;
                      if (completedCount === totalCount) {
                        callback(null);
                      }
                    });
                  }
                });
              });
            });
          });
        };

        const shuffleData = (key, keyValuePair, jobId, gid, callback) => {
          // add shuffled data to the local.store
          // using a keyname of shuffle_data_${ key }
          global.distribution[gid].store.put(JSON.stringify(keyValuePair),
              `shuffle_data_${key}_${jobId}`, (e) => {
                if (e && Object.keys(e).length > 0) {
                  console.error(
                      'Failed to save shuffle data to local storage:', e);
                  callback(e);
                  return;
                } else {
                  callback(null);
                }
              });
        };


        const reduceFunction = (keys, gid, jobId, trueReduce, callback) => {
          const result = {};
          let completedCount = 0;

          global.distribution[gid].store.get(null, (e, v) => {
            if (e && Object.keys(e).length > 0) {
              console.error('Failed to fetch data from local storage:', e);
              callback(e, null);
              return;
            } else {
              const mapOutputKeys = v.filter((key) =>
                key.startsWith(`shuffle_data_`) &&
                key.endsWith(`_${jobId}`));
              const totalCount = mapOutputKeys.length;

              mapOutputKeys.forEach((key) => {
                global.distribution[gid].store.get(key, (e, v) => {
                  if (e && Object.keys(e).length > 0) {
                    console.error(
                        'Failed to fetch data from local storage:', e);
                    callback(e, null);
                    return;
                  } else {
                    const parsedObj = JSON.parse(v);

                    if (Array.isArray(parsedObj)) {
                      parsedObj.forEach((item) => {
                        const tempKey = Object.keys(item)[0];
                        const value = item[tempKey];

                        if (result[tempKey]) {
                          result[tempKey].push(value);
                        } else {
                          result[tempKey] = [value];
                        }
                      });
                    } else {
                      Object.entries(parsedObj).forEach(
                          ([tempKey, value]) => {
                            if (result[tempKey]) {
                              result[tempKey].push(value);
                            } else {
                              result[tempKey] = [value];
                            }
                          });
                    }

                    completedCount++;

                    if (completedCount === totalCount) {
                      Object.entries(result).forEach(
                          ([key, values]) => {
                            const reducedValue = trueReduce(key, values);
                            result[key] = reducedValue;
                          });
                      callback(null, Object.values(result));
                    }
                  }
                });
              });
            }
          });
        };

        sendEndpointToAllNodes(endpoint, workerNodes, notify);
        sendFunctionsToAllNodes(mapFunction,
            reduceFunction, shuffleFunction,
            shuffleData, configuration.map,
            configuration.reduce, configuration.compact);

        // call map
        map((err, res) => {
          if (err && Object.keys(err).length > 0) {
            console.error('Failed to map:', err);
            return;
          } else {
            shuffle(() => {
              console.log('Shuffle phase completed');
              reduce((err, reduceResults) => {
                if (err && Object.keys(err).length > 0) {
                  console.error('Failed to reduce:', err);
                  return;
                } else {
                  finishJob(reduceResults, configuration.reduce);
                }
              });
            });
          }
        });
      });

      // Finish the job
      function finishJob(reduceResults, reduceFunction) {
        const combinedResults = {};
        reduceResults.forEach((result) => {
          const obj = result[0];
          Object.entries(obj).forEach(([nodeId, val]) => {
            val.forEach((item) => {
              const key = Object.keys(item)[0];
              const value = item[key];

              if (combinedResults[key]) {
                combinedResults[key].push(value);
              } else {
                combinedResults[key] = [value];
              }
            });
          });
        });

        const finalResult = Object.entries(combinedResults).
            map(([key, values]) => {
              return {[key]: values[0]};
            });

        // degregister the job from all of the nodes
        deregisterEndpoint();

        callback(null, finalResult);
      }
    },
  };
};

module.exports = mr;
