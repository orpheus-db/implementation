
  var margin = {top: 50, right: 0, bottom: 0, left: 0},
   width = 550 - margin.right - margin.left,
   height = 500 - margin.top - margin.bottom;

  var i = 0;

  var tree = d3.layout.tree()
   .size([height, width]);

  var svg = d3.select("#viz").append("svg")
   .attr("width", width + margin.right + margin.left)
   .attr("height", height + margin.top + margin.bottom)
    .append("g")
   .attr("transform", "translate(" + margin.left + "," + margin.top + ")");


var select_node = [];
function update(data){
  select_node = [];
  data = JSON.parse(data);
  var treeData = [];
  treeData.push(data);

  root = treeData[0];
  source = root;

  // Compute the new tree layout.]
  var nodes = tree.nodes(root).reverse(),
   links = tree.links(nodes);

  // Normalize for fixed-depth.
  nodes.forEach(function(d) { d.y = d.depth * 80; });

  // Declare the nodes
  var node = svg.selectAll("g.node")
   .data(nodes, function(d) { return d.id || (d.id = ++i); });

  console.log(node);
  // Enter the nodes.
  var nodeEnter = node.enter().append("g")
   .attr("class", "node")
   .attr("transform", function(d) {
    return "translate(" + d.x + "," + d.y + ")"; });

  nodeEnter.append("circle")
   .attr("r", 10)
   .attr("selected", "false")
   .style("fill", "#fff")
   .on('click', function(nodeEnter) {
      // If the node has not been selected -> change its stoke color to firebrick
      if (d3.select(this).attr("selected") == "false"){
        d3.select(this).style("stroke", "firebrick")
        d3.select(this).attr("selected", "true")
        select_node.push(nodeEnter.name)
      }else{
        d3.select(this).style("stroke", "steelblue")
        d3.select(this).attr("selected", "false")
        var index = select_node.indexOf(nodeEnter.name)
        select_node.splice(index, 1)
      }
    });

   nodeEnter.append("text")
   .attr("y", function(d) {
    return d.children || d._children ? -18 : 18; })
   .attr("dy", ".35em")
   .attr("text-anchor", "middle")
    .attr("selected", "false")
   .text(function(d) { return "vid = " + d.name; })
   .style("fill-opacity", 1);

  var diagonal = d3.svg.diagonal();

  for (var w = 0; w < nodes.length; w++) {
    var cur = nodes[w];
    if(cur.parent_) {
      for (var j=0;j<cur.parent_.length;j++) {
        var addon = {};
        addon.source = cur;
        for(var k=0; k<nodes.length;k++) {
          if (cur.parent_[j].name == nodes[k].name) {
            addon.target = nodes[k];
            break;
          }
        }
        links.push(addon);
      }
    }
  }

  // Declare the links
  var link = svg.selectAll("path.link")
   .data(links);

   link.enter().insert("path", "g")
   .attr("class", "link")
   .attr("d", diagonal);
}

function prepopulate_checkout_cmd(){
  if (select_node.length == 0){
    alert("Please select at least one version node.")
    return
  }
  var e = document.getElementById("cvd_selection");
  var selected_cvd = e.options[e.selectedIndex].text;

  var str = "orpheus checkout " + selected_cvd
  for (i = 0; i < select_node.length; i++) {
    str += " -v " + select_node[i]
  }
  str += " -f demo.csv "
  document.getElementById("cmdText").value = str;
}

function prepopulate_query_cmd(){
  if (select_node.length == 0){
    alert("Please select at least one version node.")
    return
  }
  var str = "orpheus run "
  str += "\"SELECT coexpression, count(*) FROM VERSION " + select_node.join(", ")

  var e = document.getElementById("cvd_selection");
  var selected_cvd = e.options[e.selectedIndex].text;
  str += " OF CVD " + selected_cvd + " "
  str += "GROUP BY coexpression\";"
  document.getElementById("cmdText").value = str;

}

function prepopulate_explore_cmd(){
  var str = "orpheus run "

  var e = document.getElementById("cvd_selection");
  var selected_cvd = e.options[e.selectedIndex].text;

  str += "\"SELECT vid FROM CVD " + selected_cvd
  str += " WHERE coexpression > 80 "
  str += "GROUP BY vid "
  str += "HAVING count(*) > 100 \";"

  document.getElementById("cmdText").value = str;
}

function prepopulate_view_cmd(){
  if (select_node.length == 0){
    alert("Please select at least one version node.")
    return
  }

  var str = "orpheus run "
  str += "\"SELECT * FROM VERSION " + select_node.join(",")

  var e = document.getElementById("cvd_selection");
  var selected_cvd = e.options[e.selectedIndex].text;
  str += " OF CVD " + selected_cvd + " LIMIT 200;\""
  document.getElementById("cmdExec").value = str;
}

function prepopulate_diff_cmd(){
  if (select_node.length != 2){
    alert("Please select only two version nodes to compare.")
    return
  }
  var v1 = select_node[0]
  var v2 = select_node[1]
  var e = document.getElementById("cvd_selection");
  var selected_cvd = e.options[e.selectedIndex].text;

  var str = "orpheus run "
  // str += "\"(SELECT * FROM VERSION " + v1 + " OF CVD " + selected_cvd + " EXCEPT " + " SELECT * FROM VERSION " + v2 + " OF CVD " + selected_cvd+ ")"
  // str += " UNION "
  // str += "(SELECT * FROM VERSION " + v2 + " OF CVD " + selected_cvd + " EXCEPT " + " SELECT * FROM VERSION " + v1 + " OF CVD " + selected_cvd+ ") LIMIT 200; \""
  str += "\" (SELECT * FROM CVD " + selected_cvd + " WHERE vid = " + v1 + ") EXCEPT " + "(SELECT * FROM CVD " + selected_cvd + " WHERE vid = " + v2 + ") LIMIT 200;\""

  document.getElementById("cmdExec").value = str;
}
function prepopulate_info_cmd(){
  if (select_node.length == 0){
    alert("Please select at least one version node.")
    return
  }
  var e = document.getElementById("cvd_selection");
  var selected_cvd = e.options[e.selectedIndex].text;

  //TODO: the version table name is hard-coded
  var str = "orpheus run "
  str += "\"SELECT * FROM " + selected_cvd + "_versiontable WHERE vid = ANY(ARRAY[" + select_node.join(",") + "])\""
  document.getElementById("cmdExec").value = str;

}

function prepopulate_show_cmd(){
  var e = document.getElementById("cvd_selection");
  var selected_cvd = e.options[e.selectedIndex].text;
  var str = "orpheus show " + selected_cvd
  document.getElementById("cmdExec").value = str;
}
// References:
// http://www.d3noob.org/2014/01/tree-diagrams-in-d3js_11.html