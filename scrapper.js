const cheerio = require("cheerio");
const request = require("request");

let links = {};
links["face"] = {};
links["lips"] = {};
links["eyes"] = {};

request("https://www.ulta.com", function (error, response, html) {
  if (!error && response.statusCode == 200) {
    let $ = cheerio.load(html);
    // console.log($("title"))

    console.log(JSON.stringify($(".nav-pos.ov").css("display", "block").find(".ch13-list-face")));
		$(".nav-pos.ov").css("display", "block").find(".ch13-list-face a").attr("href");
		$(".ch13-list-lips a").each((i, a)=>{links["lips"][a.innerHTML] = a.href});
		$(".ch13-list-eyes a").each((i, a)=>{links["eyes"][a.innerHTML] = a.href});
		console.log(JSON.stringify(links));
  }
});


