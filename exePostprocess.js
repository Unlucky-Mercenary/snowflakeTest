const post=require('./mymodules/postProcess');
//標準入力
function readUserInput(question) {
    const readline = require('readline').createInterface({
      input: process.stdin,
      output: process.stdout
    });
  
    return new Promise((resolve, reject) => {
      readline.question(question, (answer) => {
        resolve(answer);
        readline.close();
      });
    });
  }
const main = async ()=> {
        const sessionId=await readUserInput('sessionID:');
        
        //ヒストリーをCSV化
        post.postProcess('out4.csv','SNOWFLAKE_SAMPLE_DATA',sessionId);
}

    main();