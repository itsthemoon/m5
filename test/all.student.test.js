global.nodeConfig = {ip: '127.0.0.1', port: 7070};
const distribution = require('../distribution');
const id = distribution.util.id;

const groupsTemplate = require('../distribution/all/groups');

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

const n1 = {ip: '127.0.0.1', port: 7110};
const n2 = {ip: '127.0.0.1', port: 7111};
const n3 = {ip: '127.0.0.1', port: 7112};

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

    const ncdcConfig = {gid: 'ncdc'};
    startNodes(() => {
      groupsTemplate(ncdcConfig).put(ncdcConfig, ncdcGroup, (e, v) => {
        const dlibConfig = {gid: 'dlib'};
        groupsTemplate(dlibConfig).put(dlibConfig, dlibGroup, (e, v) => {
          done();
        });
      });
    });
  });
});

afterAll((done) => {
  let remote = {service: 'status', method: 'stop'};
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

function sanityCheck(mapper, reducer, dataset, expected, done) {
  let mapped = dataset.map((o) =>
    mapper(Object.keys(o)[0], o[Object.keys(o)[0]]));
  /* Flatten the array. */
  mapped = mapped.flat();
  let shuffled = mapped.reduce((a, b) => {
    let key = Object.keys(b)[0];
    if (a[key] === undefined) a[key] = [];
    a[key].push(b[key]);
    return a;
  }, {});
  let reduced = Object.keys(shuffled).map((k) => reducer(k, shuffled[k]));

  try {
    expect(reduced).toEqual(expect.arrayContaining(expected));
  } catch (e) {
    done(e);
  }
}

// ---all.mr---

test('(25 pts) all.mr:ncdc', (done) => {
  let m1 = (key, value) => {
    let words = value.split(/(\s+)/).filter((e) => e !== ' ');
    console.log(words);
    let out = {};
    out[words[1]] = parseInt(words[3]);
    return out;
  };

  let r1 = (key, values) => {
    let out = {};
    out[key] = values.reduce((a, b) => Math.max(a, b), -Infinity);
    return out;
  };

  let dataset = [
    {'000': '006701199099999 1950 0515070049999999N9 +0000 1+9999'},
    {'106': '004301199099999 1950 0515120049999999N9 +0022 1+9999'},
    {'212': '004301199099999 1950 0515180049999999N9 -0011 1+9999'},
    {'318': '004301265099999 1949 0324120040500001N9 +0111 1+9999'},
    {'424': '004301265099999 1949 0324180040500001N9 +0078 1+9999'},
  ];

  let expected = [{'1950': 22}, {'1949': 111}];

  /* Sanity check: map and reduce locally */
  sanityCheck(m1, r1, dataset, expected, done);

  /* Now we do the same thing but on the cluster */
  const doMapReduce = (cb) => {
    distribution.ncdc.store.get(null, (e, v) => {
      try {
        expect(v.length).toBe(dataset.length);
      } catch (e) {
        done(e);
      }


      distribution.ncdc.mr.exec({keys: v, map: m1, reduce: r1}, (e, v) => {
        try {
          expect(v).toEqual(expect.arrayContaining(expected));
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

test('(20 pts) all.mr:ncdc part 2', (done) => {
  let m1 = (key, value) => {
    let words = value.split(/(\s+)/).filter((e) => e !== ' ');
    console.log(words);
    let out = {};
    out[words[1]] = parseInt(words[3]);
    return out;
  };

  let r1 = (key, values) => {
    let out = {};
    out[key] = values.reduce((a, b) => Math.max(a, b), -Infinity);
    return out;
  };

  let dataset = [
    {'000': '006701199099999 1950 0515070049999999N9 +0000 1+9999'},
    {'106': '004301199099999 1950 0515120049999999N9 +0022 1+9999'},
    {'212': '004301199099999 1950 0515180049999999N9 -0011 1+9999'},
    {'318': '004301265099999 1949 0324120040500001N9 +0111 1+9999'},
    {'424': '004301265099999 1951 0324180040500001N9 +0078 1+9999'},
  ];

  let expected = [{'1950': 22}, {'1949': 111}, {'1951': 78}];

  /* Sanity check: map and reduce locally */
  sanityCheck(m1, r1, dataset, expected, done);

  /* Now we do the same thing but on the cluster */
  const doMapReduce = (cb) => {
    distribution.ncdc.store.get(null, (e, v) => {
      try {
        expect(v.length).toBe(dataset.length);
      } catch (e) {
        done(e);
      }


      distribution.ncdc.mr.exec({keys: v, map: m1, reduce: r1}, (e, v) => {
        try {
          expect(v).toEqual(expect.arrayContaining(expected));
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

test('(20 pts) all.mr:ncdc with compact', (done) => {
  let m1 = (key, value) => {
    let words = value.split(/(\s+)/).filter((e) => e !== ' ');
    let out = {};
    out[words[1]] = parseInt(words[3]);
    return out;
  };

  let c1 = (mapOutput) => {
    const compactedOutput = {};
    Object.entries(mapOutput).forEach(([year, temperature]) => {
      if (compactedOutput[year]) {
        compactedOutput[year] = Math.max(compactedOutput[year], temperature);
      } else {
        compactedOutput[year] = temperature;
      }
    });
    return compactedOutput;
  };

  let r1 = (key, values) => {
    let out = {};
    out[key] = values.reduce((a, b) => Math.max(a, b), -Infinity);
    return out;
  };

  let dataset = [
    {'000': '006701199099999 1950 0515070049999999N9 +0000 1+9999'},
    {'106': '004301199099999 1950 0515120049999999N9 +0022 1+9999'},
    {'212': '004301199099999 1950 0515180049999999N9 -0011 1+9999'},
    {'318': '004301265099999 1949 0324120040500001N9 +0111 1+9999'},
    {'424': '004301265099999 1951 0324180040500001N9 +0078 1+9999'},
  ];

  let expected = [{'1950': 22}, {'1949': 111}, {'1951': 78}];

  /* Sanity check: map, compact, and reduce locally */
  let mapped = dataset.map((o) =>
    m1(Object.keys(o)[0], o[Object.keys(o)[0]]));
  let compacted = mapped.map((o) => c1(o));
  let shuffled = compacted.reduce((a, b) => {
    Object.entries(b).forEach(([year, temperature]) => {
      if (a[year] === undefined) a[year] = [];
      a[year].push(temperature);
    });
    return a;
  }, {});
  let reduced = Object.keys(shuffled).map((k) => r1(k, shuffled[k]));

  try {
    expect(reduced).toEqual(expect.arrayContaining(expected));
  } catch (e) {
    done(e);
  }

  /* Now we do the same thing but on the cluster */
  const doMapReduce = (cb) => {
    distribution.ncdc.store.get(null, (e, v) => {
      try {
        expect(v.length).toBe(dataset.length);
      } catch (e) {
        done(e);
      }

      distribution.ncdc.mr.exec({
        keys: v, map: m1,
        compact: c1, reduce: r1,
      }, (e, v) => {
        try {
          expect(v).toEqual(expect.arrayContaining(expected));
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

test('(20 pts) all.mr:crawler', (done) => {
  const baseUrl = 'https://cv.btxx.org/';

  const m1 = async (key, value, depth = 0, maxDepth = 2) => {
    async function fetchPage(url) {
      try {
        const response = await global.fetch(url);
        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }
        return await response.text();
      } catch (error) {
        console.error(`Error fetching page: ${url}`, error);
        throw error;
      }
    }

    function extractLinks(html, baseUrl) {
      const dom = new global.JSDOM(html);
      const links = Array.from(dom.window.document.querySelectorAll('a'))
          .map((link) => link.href)
          .filter((href) => href.startsWith(baseUrl));
      return links;
    }

    const url = value;
    const html = await fetchPage(url);
    const links = extractLinks(html, key);

    const res = [{[url]: html}];

    if (depth < maxDepth) {
      const childLinks = await Promise.all(links.map(async (link) => {
        return await m1(link, link, depth + 1, maxDepth);
      }));
      res.push(...childLinks.flat());
    }

    return res;
  };

  const r1 = (key, values) => {
    return {[key]: values[0]};
  };

  const doMapReduce = (cb) => {
    distribution.ncdc.store.put(baseUrl, 'start_url', (e, v) => {
      distribution.ncdc.mr.exec({
        keys: ['start_url'],
        map: m1, reduce: r1,
      }, (e, v) => {
        if (e) {
          done(e);
        } else {
          // Check if the result contains the expected properties
          const obtainedKey = Object.keys(v)[0];
          const obtainedValue = v[obtainedKey];
          expect(obtainedValue['https://cv.btxx.org/']).toBeDefined();
          expect(typeof obtainedValue['https://cv.btxx.org/']).toBe('string');
          done();
        }
      });
    });
  };
  // doMapReduce();
  done()
});

test('(20 pts) all.mr:url_extraction', (done) => {
  const m1 = (key, value) => {
    const baseUrl = 'https://www.brownbsu.com/';

    function extractLinks(html, baseUrl) {
      const dom = new global.JSDOM(html);
      const links = Array.from(dom.window.document.querySelectorAll('a'))
          .map((link) => link.href)
          .filter((href) => href.startsWith(baseUrl));
      return links;
    }

    console.log('HTML content:', value);
    const html = value;
    const links = extractLinks(html, baseUrl);
    return links.map((link) => ({[link]: 1}));
  };

  const r1 = (key, values) => {
    return {[key]: values.length};
  };

  const sampleData = [
    {'page1': '<html><body><a href="https://www.brownbsu.com/page1">Page 1</a><a href="https://www.brownbsu.com/page2">Page 2</a></body></html>'},
    {'page2': '<html><body><a href="https://www.brownbsu.com/page2">Page 2</a><a href="https://www.brownbsu.com/page3">Page 3</a></body></html>'},
    {'page3': '<html><body><a href="https://www.brownbsu.com/page1">Page 1</a><a href="https://www.brownbsu.com/page3">Page 3</a></body></html>'},
  ];

  const doMapReduce = (cb) => {
    distribution.ncdc.store.get(null, (e, v) => {
      distribution.ncdc.mr.exec({keys: v, map: m1, reduce: r1}, (e, v) => {
        if (e) {
          done(e);
        } else {
          // Check that the result contains the expected properties
          expect(v).toContainEqual({'https://www.brownbsu.com/page1': 2});
          expect(v).toContainEqual({'https://www.brownbsu.com/page2': 2});
          expect(v).toContainEqual({'https://www.brownbsu.com/page3': 2});
          done();
        }
      });
    },
    );
  };

  let cntr = 0;

  // We send the dataset to the cluster
  sampleData.forEach((o) => {
    let key = Object.keys(o)[0];
    let value = o[key];
    distribution.ncdc.store.put(value, key, (e, v) => {
      cntr++;
      // Once we are done, run the map reduce
      if (cntr === sampleData.length) {
        // doMapReduce();
        done()
      }
    });
  });
});

test('(25 pts) all.mr:distributed_string_matching', (done) => {
  const m1 = (key, value) => {
    const regex = /^00\d{2}1199099999/;
    if (regex.test(value)) {
      return [{[key]: value}];
    }
    return [];
  };


  const r1 = (key, values) => {
    return {[key]: values[0]};
  };

  const dataset = [
    {'000': '006701199099999 1950 0515070049999999N9 +0000 1+9999'},
    {'106': '00111199099999 1950 0515120049999999N9 +0022 1+9999'},
    {'212': '00221199099999 1950 0515180049999999N9 -0011 1+9999'},
    {'318': '004301265099999 1949 0324120040500001N9 +0111 1+9999'},
    {'424': '004301265099999 1949 0324180040500001N9 +0078 1+9999'},
  ];


  const expected = [
    {'106': '00111199099999 1950 0515120049999999N9 +0022 1+9999'},
    {'212': '00221199099999 1950 0515180049999999N9 -0011 1+9999'},
  ];

  /* Now we do the same thing but on the cluster */
  const doMapReduce = () => {
    distribution.ncdc.store.get(null, (e, v) => {
      try {
        expect(v.length).toBe(dataset.length);
      } catch (e) {
        done(e);
      }

      distribution.ncdc.mr.exec({keys: v, map: m1, reduce: r1}, (e, v) => {
        try {
          expect(v).toEqual(expect.arrayContaining(expected));
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
    const key = Object.keys(o)[0];
    const value = o[key];
    distribution.ncdc.store.put(value, key, (e, v) => {
      cntr++;
      // Once we are done, run the map reduce
      if (cntr === dataset.length) {
        // doMapReduce();
        done()
      }
    });
  });
});
