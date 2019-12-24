function barPlot() {
    var data = [80, 100, 56, 120, 180, 30, 40, 120, 160];
    var svgWidth = 500, svgHeight = 300;
    var barPadding = 5, textPadding = 2;
    var barWidth = (svgWidth / data.length); 
    // The required padding between bars is 5px.
    // The label must locate 2px above the middle of each bar.

    var svg = d3.select('svg')
            .attr("width", svgWidth)
            .attr("height", svgHeight);

    var barChart = svg.selectAll("rect")
            .data(data)
            .enter()
            .append("rect")
            .attr("class", "bar")
            .attr("transform", function (d, i) {  
                var xCoordinate = barWidth * i;
                var yCoordinate = svgHeight - d;
                return "translate(" + xCoordinate + "," + yCoordinate + ")";  
            })
            .attr('width', barWidth - barPadding)
            .attr('height', function(d) {return d})
            .attr("fill", "#CC6450");

    barText = svg.selectAll("text")
            .data(data)
            .enter()
            .append("text")
            .text(function(d) {return d;})
            .attr("transform", function (d, i) {  
                var xCoordinate = barWidth * (i + 1/2) - barPadding / 2;
                var yCoordinate = svgHeight - d - textPadding;
                return "translate(" + xCoordinate + "," + yCoordinate + ")";  
            })
            .attr("text-anchor", "middle");
}