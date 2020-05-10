//var dataset = {{ data | safe }};
//console.log(dataset)

var margin = {top: 20, right: 20, bottom: 70, left: 40},
width = 600 - margin.left - margin.right,
height = 300 - margin.top - margin.bottom;

var x = d3.scale.ordinal().rangeRoundBands([0, width], .05);
var y = d3.scale.linear().range([height, 0]);

var xAxis = d3.svg.axis()
.scale(x)
.orient("bottom")
var yAxis = d3.svg.axis()
.scale(y)
.orient("left")
.ticks(10);

var svg = d3.select("body").append("svg")
.attr("width", width + margin.left + margin.right)
.attr("height", height + margin.top + margin.bottom)
.append("g")
.attr("transform", 
      "translate(" + margin.left + "," + margin.top + ")");

function drawGraph(data) {
// For each row in the data, parse the date
// and use + to make sure data is numerical
  data = JSON.parse(data);
  data.forEach(function(d) {
  x.domain(data.map(function(d) { return d.prediction; }));
  y.domain(data.map(function(d) { return d.counts; })); 
  });

  svg.append("g")
  .attr("class", "x axis")
  .attr("transform", "translate(0," + height + ")")
  .call(xAxis)

  svg.append("g")
  .attr("class", "y axis")
  .call(yAxis)
  .append("text")
  .attr("transform", "rotate(-90)")
  .attr("y", 6)
  .attr("dy", ".71em")
  .style("text-anchor", "end")
  .text("count");

  svg.selectAll(".bar")
  .data(data)
.enter().append("rect")
.attr("class", "bar")
  .attr("x", function(d) { return x(d.prediction); })
  .attr("width", x.rangeBand())
  .attr("y", function(d) { return y(d.counts); })
  .attr("height", function(d) { return height - y(d.counts); });


  svg.selectAll("text.bar")
  .data(data)
.enter().append("text")
  .attr("class", "bar")
  .attr("text-anchor", "middle")
  .attr("x", function(d) { return x(d.prediction) + x.rangeBand()/2; })
  .attr("y", function(d) { return y(d.counts) - 5; })
  .text(function(d) { return d.counts; });
} 

drawGraph(dataset);
