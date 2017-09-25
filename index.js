const RedisSMQ = require('rsmq');
/**
 * Wrapper around RSMQ, simplify working queue creation, management etc
 * 
 * @class Queue
 */
class Queue {
  constructor (opts) {
    this.rsmq = new RedisSMQ(opts);
    this.createdChannel = null;
  }
  create (opts) {
    if (typeof opts === 'string') opts = { qname: opts }
    const { qname } = opts;
    return this.exists(qname)
    .then( exists => {
      if (exists) {
        this.createdChannel = qname;
        let tmp = Object.assign({}, opts);
        delete tmp.qname;
        if (Object.keys(tmp).length) {
          return this.setAttributes(tmp);
        }
  
        return Promise.resolve(true);
      }
      return new Promise((resolve, reject) => {
        this.rsmq.createQueue(opts, (err, resp) => {
          if (err) return reject(err);
          this.createdChannel = qname;
          return resolve(!!resp);
        });
      });
    })
    .catch(err => {
      throw err;
    });
  }
  exists (qName) {
    return new Promise((resolve, reject) => {
      this.rsmq.listQueues((err, queues) => {
        if (err) return reject(err);
        return resolve(queues.includes(qName));
      });
    });
  }
  send (message) {
    if (!this.createdChannel)
      return Promise.reject(new Error('You must first create a queue.'));
    return new Promise((resolve, reject) => {
      this.rsmq.sendMessage(
        {
          qname: this.createdChannel,
          message:
            typeof message === 'object' ? JSON.stringify(message) : message
        },
        (err, resp) => {
          if (err) return reject(err);
          return resolve(resp);
        }
      );
    });
  }
  receiveMessage () {
    if (!this.createdChannel)
      return Promise.reject(new Error('You must first create a queue.'));
    return new Promise((resolve, reject) => {
      this.rsmq.receiveMessage({ qname: this.createdChannel }, (err, resp) => {
        if (err) return reject(err);
        return resolve(JSON.parse(resp));
      });
    });
  }
  deleteMessage (messageId) {
    if (!this.createdChannel)
      return Promise.reject(new Error('You must first create a queue.'));
    return new Promise((resolve, reject) => {
      this.rsmq.deleteMessage(
        { qname: this.createdChannel, id: messageId },
        function (err, resp) {
          if (err) return reject(err);
          return resolve(!!resp);
        }
      );
    });
  }
  popMessage () {
    if (!this.createdChannel)
      return Promise.reject(new Error('You must first create a queue.'));
    return new Promise((resolve, reject) => {
      this.rsmq.popMessage({ qname: this.createdChannel }, (err, resp) => {
        if (err) return reject(err);
        return resolve(JSON.parse(resp));
      });
    });
  }
  destroy () {
    if (!this.createdChannel)
      return Promise.reject(new Error('You must first create a queue.'));
    return new Promise((resolve, reject) => {
      this.rsmq.deleteQueue({ qname: this.createdChannel }, (err, resp) => {
        if (err) return reject(err);
        return resolve(!!resp);
      });
    });
  }
  setAttributes (attrs) {
    let tmp = Object.assign({}, attrs);
    delete tmp.qname;
    if (!Object.keys(tmp).length)
      return Promise.reject(
        new Error(
          'You must at least provide an attribute to set (vt, delay, maxsize)'
        )
      );
    if (!this.createdChannel)
      return Promise.reject(new Error('You must first create a queue.'));
    return new Promise((resolve, reject) => {
      this.rsmq.setQueueAttributes(Object.assign({}, attrs, { qname: this.createdChannel }),
        (err, resp) => {
          if (err) return reject(err);
          return resolve(!!resp);
        }
      );
    });
  }
}

module.exports = Queue;
