//PRISMA is an object relational mapping tool-->helps us to interact with db easily by using js objects
//see z_images/01.png ---> ye hai hmari raw sql query which we will use to fetch pertiicular data from db
//see 02.png --->isme dekho we have created wuery in 01.png into js object form by using orm tools(like prisma) and this is better way coz it is better incase the query is big,makes query more readable,provides clean api structure\
//we are using prisma as orm tool coz i works well with postgres db

const { PrismaClient } = require('@prisma/client');
const { PrismaPg } = require('@prisma/adapter-pg');
const {config} = require('./');
const connectionString = config.DATABASE_URL;

//YHA PR SIMILARLY LIKE redis.js WE ARE USING SINGLETON DESIGN PATTERN WHICH ENSURES KI HAMARE PAAS SINGLE PRISMA INSTANCE AVAILABLE HO AND HELP US TO AVOID CREATING MULTIPLE PRISMA INSTANCES
const globalForPrisma = global;

if (!globalForPrisma.prisma) {
    const adapter = new PrismaPg({ connectionString });

  globalForPrisma.prisma = new PrismaClient({
    adapter,
    log: ['error', 'warn'],
  });
}

const prisma = globalForPrisma.prisma;

module.exports = prisma;
