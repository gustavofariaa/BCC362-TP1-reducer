"use strict";

const {
  DbReduce,
  lockDB,
  unlockDB,
  connect,
} = require("@joaoderocha/redis-pub-sub-nodejs/headless/database");

const {
  roundRobinSubscribe,
  REDUCE,
} = require("@joaoderocha/redis-pub-sub-nodejs/headless/utils");

const somaMapas = (mapa1, mapa2) => {
  return Object.entries(mapa1).reduce((acc, [key, value]) => {
    if (acc[key]) {
      acc[key] += value;
    } else {
      acc[key] = value;
    }

    return acc;
  }, mapa2);
};

const reducer = async (channel, message) => {
  let numeroDeDocumentos = await DbReduce.total();
  if (numeroDeDocumentos === 0) {
    return DbReduce.cadastrar({ canal: fila, message });
  }
  await lockDB();
  numeroDeDocumentos = await DbReduce.total();
  if (numeroDeDocumentos === 0) {
    await unlockDB();
    return DbReduce.cadastrar({ canal: fila, message });
  }
  const { message: obj } = await DbReduce.findOneAndRemove({});

  await unlockDB();

  const { linha } = message;

  console.log(DbReduce.cadastrar(somaMapas(linha, obj)));
};

connect();

roundRobinSubscribe(REDUCE, reducer);
