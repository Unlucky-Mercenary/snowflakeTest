const Snowflake = require('snowflake-promise').Snowflake;
const config=require('./config/config');
const exe=require('./mymodules/execute.js');
const post=require('./mymodules/postProcess');

    const main = async ()=> {
        //コネクション確立
        const connection = new Snowflake({
            account: config.ACCOUNTNAME,
            username: config.USERNAME,
            password: config.PASSWORD,
            warehouse:warehouse,
            database:'SNOWFLAKE_SAMPLE_DATA',
            schema:'tpch_sf1'
            }
            );
        await connection.connect();

        //sessionId取得
        row=await exe.exeSingleQuery(connection,'select current_session()');
        const sessionId=row[0]['CURRENT_SESSION()'];
        //前処理
        await Promise.all([
            exe.exeSingleQuery(connection,'alter session set use_cached_result=false')
          ]);
        
        //処理
        await exe.exeMultiQuery(connection,0,"select l_returnflag, l_linestatus,sum(l_quantity) as sum_qty,sum(l_extendedprice) as sum_base_price,sum(l_extendedprice * (1-l_discount)) as sum_disc_price,sum(l_extendedprice * (1-l_discount) * (1+l_tax)) as sum_charge,avg(l_quantity) as avg_qty,avg(l_extendedprice) as avg_price,avg(l_discount) as avg_disc,count(*) as count_order from lineitem where l_shipdate <= dateadd(day, -90, to_date('1998-12-01')) group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus;");
        
        //後処理
        await Promise.all([
            exe.exeSingleQuery(connection,'alter session set use_cached_result=true')
          ]);
        

        //await exe.exeSingleQuery(connection,'ALTER WAREHOUSE IF EXISTS KAYANO_DATA SUSPEND');

        //ヒストリーをCSV化
        post.postProcess('out9.csv','SNOWFLAKE_SAMPLE_DATA',sessionId);

    }


    main();