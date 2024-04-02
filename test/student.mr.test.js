const distribution = require('../../m5/distribution');
const id = distribution.util.id;

global.nodeConfig = { ip: '127.0.0.1', port: 7070 };
const groupsTemplate = require('../../m5/distribution/all/groups');

const ncdcGroup = {};
const dlibGroup = {};

/*
   This hack is necessary since we can not
   gracefully stop the local listening node.
   The process that node is
   running in is the actual jest process
*/
let localServer = null;

/*
    The local node will be the orchestrator.
*/

const n1 = { ip: '127.0.0.1', port: 7110 };
const n2 = { ip: '127.0.0.1', port: 7111 };
const n3 = { ip: '127.0.0.1', port: 7112 };

beforeAll((done) => {
    /* Stop the nodes if they are running */

    ncdcGroup[id.getSID(n1)] = n1;
    ncdcGroup[id.getSID(n2)] = n2;
    ncdcGroup[id.getSID(n3)] = n3;

    dlibGroup[id.getSID(n1)] = n1;
    dlibGroup[id.getSID(n2)] = n2;
    dlibGroup[id.getSID(n3)] = n3;

    const startNodes = (cb) => {
        distribution.local.status.spawn(n1, (e, v) => {
            distribution.local.status.spawn(n2, (e, v) => {
                distribution.local.status.spawn(n3, (e, v) => {
                    cb();
                });
            });
        });
    };

    distribution.node.start((server) => {
        localServer = server;

        const ncdcConfig = { gid: 'ncdc' };
        startNodes(() => {
            groupsTemplate(ncdcConfig).put(ncdcConfig, ncdcGroup, (e, v) => {
                const dlibConfig = { gid: 'dlib' };
                groupsTemplate(dlibConfig).put(dlibConfig, dlibGroup, (e, v) => {
                    done();
                });
            });
        });
    });
});

afterAll((done) => {
    let remote = { service: 'status', method: 'stop' };
    remote.node = n1;
    distribution.local.comm.send([], remote, (e, v) => {
        remote.node = n2;
        distribution.local.comm.send([], remote, (e, v) => {
            remote.node = n3;
            distribution.local.comm.send([], remote, (e, v) => {
                localServer.close();
                done();
            });
        });
    });
});

test('MapReduce Setup Phase', (done) => {
    const configuration = {
        keys: ['key1', 'key2', 'key3'],
        map: (key, value) => {
            // Sample map function
            return { [key]: value.toUpperCase() };
        },
        reduce: (key, values) => {
            // Sample reduce function
            return { [key]: values.join(',') };
        },
    };

    distribution.ncdc.mr.exec(configuration, (e, v) => {
        // check if the coordinator endpoint is registered
        const coordinatorEndpoints = Object.keys(global.distribution.local).filter(
            (key) => key.includes('coordinator')
        );
        expect(coordinatorEndpoints.length).toBe(1);

        // check if the worker endpoints are registered
        const workerEndpoints = Object.keys(global.distribution.local).filter(
            (key) => key.includes('worker')
        );
        expect(workerEndpoints.length).toBe(Object.keys(ncdcGroup).length - 1);
        done();
    });
});

test('MapReduce Map Phase', (done) => {
    const dataset = [
        { '000': '006701199099999 1950 0515070049999999N9 +0000 1+9999' },
        { '106': '004301199099999 1950 0515120049999999N9 +0022 1+9999' },
        { '212': '004301199099999 1950 0515180049999999N9 -0011 1+9999' },
        { '318': '004301265099999 1949 0324120040500001N9 +0111 1+9999' },
        { '424': '004301265099999 1949 0324180040500001N9 +0078 1+9999' },
    ];

    // Store the dataset in the distributed storage
    dataset.forEach((item) => {
        const key = Object.keys(item)[0];
        const value = item[key];
        distribution.ncdc.store.put(value, key, (e, v) => {
            if (e) {
                console.error('Failed to store data:', e);
            }
        });
    });

    const m1 = (key, value) => {
        let words = value.split(/(\s+)/).filter((e) => e !== ' ');
        console.log(words);
        let out = {};
        out[words[1]] = parseInt(words[3]);
        return out;
    };

    /* Now we do the same thing but on the cluster */
    const doMapReduce = (cb) => {
        distribution.ncdc.store.get(null, (e, v) => {
            try {
                expect(v.length).toBe(dataset.length);
            } catch (e) {
                done(e);
            }


            distribution.ncdc.mr.exec({ keys: v, map: m1, reduce: null }, (e, v) => {
                try {
                    console.log("lets check ", v)
                    expect(v).toEqual(null);
                    done();
                } catch (e) {
                    done(e);
                }
            });
        });
    };

    let cntr = 0;

    // We send the dataset to the cluster
    dataset.forEach((o) => {
        let key = Object.keys(o)[0];
        let value = o[key];
        distribution.ncdc.store.put(value, key, (e, v) => {
            cntr++;
            // Once we are done, run the map reduce
            if (cntr === dataset.length) {
                doMapReduce();
            }
        });
    });
});
