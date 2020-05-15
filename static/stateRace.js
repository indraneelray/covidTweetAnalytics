// set the dimensions and margins of the graph
var margin = {top: 20, right: 20, bottom: 30, left: 50},
    width = 960 - margin.left - margin.right,
    height = 500 - margin.top - margin.bottom;

// parse the date / time
var parseTime = d3.timeParse("%d-%b-%y");

// set the ranges
var x = d3.scaleTime().range([0, width]);
var y = d3.scaleLinear().range([height, 0]);

// define the 1st line
var valueline = d3.line()
    .x(function(d) { return x(d.date); })
    .y(function(d) { return y(d.avg_retail_and_recreation_percent_change); });

// define the 2nd line
var valueline2 = d3.line()
    .x(function(d) { return x(d.date); })
    .y(function(d) { return y(d.avg_grocery_and_pharmacy_percent_change); });


// append the svg obgect to the body of the page
// appends a 'group' element to 'svg'
// moves the 'group' element to the top left margin
var svg = d3.select("body").append("svg")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
  .append("g")
    .attr("transform",
          "translate(" + margin.left + "," + margin.top + ")");


function drawGraph(data) {

    // format the data
  data.forEach(function(d) {
    d.date = parseTime(d.date);
    d.avg_retail_and_recreation_percent_change = d.avg_retail_and_recreation_percent_change;
    d.avg_grocery_and_pharmacy_percent_change = d.avg_grocery_and_pharmacy_percent_change;
    });

    // Scale the range of the data
    x.domain(d3.extent(data, function(d) { return d.date; }));
    y.domain([0, d3.max(data, function(d) {
    return Math.max(d.avg_retail_and_recreation_percent_change, d.avg_grocery_and_pharmacy_percent_change); })]);

      // Add the valueline path.
  svg.append("path")
  .data([data])
  .attr("class", "line")
  .attr("d", valueline);

// Add the valueline2 path.
svg.append("path")
  .data([data])
  .attr("class", "line")
  .style("stroke", "red")
  .attr("d", valueline2);

// Add the X Axis
svg.append("g")
  .attr("transform", "translate(0," + height + ")")
  .call(d3.axisBottom(x));

// Add the Y Axis
svg.append("g")
  .call(d3.axisLeft(y));
    
}    

names = new Set(data.map(d => d.sub_region_1))


// datevalues = Array.from(d3.rollup(data, ([d]) => d.avg_retail_and_recreation_percent_change, d => +d.date, d => d.sub_region_1))
//   .map(([date, data]) => [new Date(date), data])
//   .sort(([a], [b]) => d3.ascending(a, b))

console.log(data);
drawGraph(data);