function renderModalHTML(data) {
    var satDetails = '';

    if(data.satelliteList.length > 0) {
        satDetails += '<div class="panel panel-default">' +
            '<div class="panel-heading">Attributes</div>' +
            '<div class="panel-body"><div class="table-responsive"><table class="table">' +
            '<thead><tr>' +
            '<th>#</th>' +
            '<th>Satellite</th>' +
            '<th>Name</th>' +
            '<th>Domain</th>' +
            '<th>Comments</th>' +
            '</tr></thead><tbody>';

        var i = 1;

        for (var satNumber in data.satelliteList) {
            var sat = data.satelliteList[satNumber];
            for (var colNumber in sat.columns) {
                var col = sat.columns[colNumber];
                satDetails += '<tr>' +
                    '<td>' + i + '</td>' +
                    '<td>' + sat.name + '</td>' +
                    '<td>' + col.name + '</td>' +
                    '<td>' + col.dataDomain.name + ' <span class="label label-default">' + col.dataDomain.sqlType + '</span></td>' +
                    '<td>' + col.comments.join(" ,") + '</td>' +
                    '</tr>';
                i = i + 1;
            }
        }

        satDetails += "</tbody></table></div></div></div>";
    }

    var objectDetails = '';

    if(data.parent.comments.length > 0)
        objectDetails = '<p><strong>Comments:</strong> ' + data.parent.comments.join(", ") + "</p>";

    if(data.parent.hub) {
        objectDetails += '<div class="panel panel-default">' +
            '<div class="panel-heading">Key columns</div>' +
            '<div class="panel-body"><div class="table-responsive"><table class="table">' +
            '<thead><tr>' +
            '<th>#</th>' +
            '<th>Name</th>' +
            '<th>Domain</th>' +
            '<th>Comments</th>' +
            '</tr></thead><tbody>';

        var i = 1;

        for(var colNumber in data.parent.columns) {
            var col = data.parent.columns[colNumber];
            objectDetails += '<tr>' +
                '<td>' + i + '</td>' +
                '<td>' + col.name + '</td>' +
                '<td>' + col.dataDomain.name + ' <span class="label label-default">' + col.dataDomain.sqlType + '</span></td>' +
                '<td>' + col.comments.join(", ") + '</td>' +
                '</tr>';
            i = i + 1;
        }

        objectDetails += "</tbody></table></div></div></div>";
    }

    if(data.parent.link) {
        objectDetails += '<div class="panel panel-default">' +
            '<div class="panel-heading">Legs</div>' +
            '<div class="panel-body"><div class="table-responsive"><table class="table">' +
            '<thead><tr>' +
            '<th>#</th>' +
            '<th>Hub</th>' +
            '<th>Role</th>' +
            '<th>Driving</th>' +
            '</tr></thead><tbody>';
        var i = 0;

        var legDict = {};  // Jackson @id stored here...

        for(var legNo in data.parent.legs) {
            var leg = data.parent.legs[legNo];

            var hub = leg.hub;

            if(leg.hub.dbId === undefined)
                hub = legDict[leg.hub];
            else
                legDict[leg.hub['@id']] = leg.hub;

            objectDetails += '<tr>' +
                '<td>' + i + '</td>' +
                '<td>' + hub.namePrefix + hub.name + '</td>' +
                '<td>' + leg.role + '</td>' +
                '<td>' + (leg.driving ? '<i class="fa fa-check"></i>' : '' ) + '</td>' +
                '</tr>';
            i = i + 1;

        }

        objectDetails += "</tbody></table></div></div></div>";
    }

    return objectDetails + satDetails;
}

function renderObjectDetailsModal(url, id, type) {
    var dialog = bootbox.dialog({
        title: 'Object details',
        size: 'large',
        message: '<p><i class="fa fa-spin fa-spinner"></i> Loading...</p>'
    });

    dialog.init(function() {
        $.ajax({
            dataType: "json",
            url: url + type + '/id/' + id,
            success: function (data) {
                var title = 'Details for ' + data.parent.namePrefix + data.parent.name;
                dialog.find('.modal-title').html(title);
                var result = renderModalHTML(data);
                dialog.find('.bootbox-body').html(result);
            }
        });
    });
}

function chefMakeId(length) {
    var text = "";
    var possible = "ABCDEFGHJKMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz123456789";

    for( var i=0; i < length; i++ )
        text += possible.charAt(Math.floor(Math.random() * possible.length));

    return text;
}

function promptRollback(jobId) {
    var rid = chefMakeId(5);

    bootbox.prompt("Rollback? Type '" + rid + "'", function (result) {
        if(rid === result) {
            $.ajax({
                url: "/mapping/rollback/" + jobId,
                success: function (data) {
                    bootbox.alert("Rollback queued with run ID " + data);
                }
            });
        }
    });
}

function promptMartRollback(jobId) {
    var rid = chefMakeId(5);

    bootbox.prompt("Rollback? Type '" + rid + "'", function (result) {
        if(rid === result) {
            $.ajax({
                url: "/mart/rollback/" + jobId,
                success: function (data) {
                    bootbox.alert("Rollback queued with run ID " + data);
                }
            });
        }
    });
}

function promtTrigger(mappingName) {
    var rid = chefMakeId(5);

    bootbox.prompt("Execute Mapping " + mappingName + "? Type '" + rid + "'", function (result) {
        if(rid === result) {
            $.ajax({
                dataType: "json",
                url: "/trigger/" + mappingName,
                success: function (data) {
                    console.log("Mapping '" + mappingName + "' triggered");
                }
            });
        }
    });
}

function promtMartTrigger(martName) {
    var rid = chefMakeId(5);

    bootbox.prompt("Execute Mart " + martName + "? Type '" + rid + "'", function (result) {
        if(rid === result) {
            $.ajax({
                dataType: "json",
                url: "/trigger/" + martName,
                success: function (data) {
                    console.log("Mart '" + martName + "' triggered");
                }
            });
        }
    });
}

function renderModelGraphFull(objectDetailUrl, loadRepo, repoDataUrl, container_id, nodesArray, edgesArray) {
    var nodes = new vis.DataSet(nodesArray);
    var edges = new vis.DataSet(edgesArray);

    var container = document.getElementById(container_id);
    var data = {nodes: nodes, edges: edges};
    var options = {
        interaction: {
            hover: true
        },
        layout: {
            improvedLayout: true
        },
        physics: {
            enabled: true,
            solver: 'forceAtlas2Based',
            stabilization: true
        }
    };
    var network = new vis.Network(container, data, options);

    network.on("click", function (params) {
        if (params.nodes.length == 0)
            return;

        var id = params['nodes'][0];
        var node = nodes.get(id);

        if(node.data == null || node.data == "SAT")
            return;

        renderObjectDetailsModal(objectDetailUrl, id, node.data);
    });

    if(loadRepo) {
        $.ajax({
            dataType: "json",
            url: repoDataUrl,
            success: function (data) {
                nodes.clear();
                edges.clear();
                nodes.add(data.nodes);
                edges.add(data.edges);
                network.stabilize();
            }
        });
    }

    return network;
}

function renderModelGraph(objectDetailsUrl, repoDataUrl, container_id) {
    var nodeArray = [];
    var edgeArray = [];
    return renderModelGraphFull(objectDetailsUrl, true, repoDataUrl, container_id, nodeArray, edgeArray);
}

function showSqlModal(title, sqlDivId) {
    var dialog = bootbox.dialog({
        title: title,
        size: 'large',
        message: '<p><i class="fa fa-spin fa-spinner"></i> Loading...</p>'
    });
    dialog.init(function() {
        var sqlCode = $("#" + sqlDivId).html();
        sqlCode = sqlFormatter.format(sqlCode, 'SQL');
        sqlCode = escapeHtmlAngelBrackets(sqlCode);
        dialog.find('.bootbox-body').html('<pre><code class="sql">' + sqlCode + '</code></pre>');

        $('pre code').each(function(i, block) {
            hljs.highlightBlock(block);
        });
    });
}

function escapeHtmlAngelBrackets(code) {
    var htmlPrintedSqlCode = code.toString().replace("& lt;", "<").replace("& gt;", ">");
    return htmlPrintedSqlCode
}


function showErrorMessage(msgContainer) {
    var msg = $("#" + msgContainer).html();
    bootbox.alert({message: msg, size: 'large' });
}

function humanFileSize(bytes, si) {
    var thresh = si ? 1000 : 1024;
    if(Math.abs(bytes) < thresh) {
        return bytes + ' B';
    }
    var units = si
        ? ['kB','MB','GB','TB','PB','EB','ZB','YB']
        : ['KiB','MiB','GiB','TiB','PiB','EiB','ZiB','YiB'];
    var u = -1;
    do {
        bytes /= thresh;
        ++u;
    } while(Math.abs(bytes) >= thresh && u < units.length - 1);
    return bytes.toFixed(1)+' '+units[u];
}

function isEmpty(str) {
    return (!str || 0 === str.length);
}

function developCallback() {
    var data = { code : editor.getValue() }

    if(!data.code || 0 === data.code.length || isEmpty(data.code)) return;

    $('#dev-btn-run').html('<i class="fa fa-spin fa-spinner"></i> Simulating...');
    $('#dev-btn-run').prop('disabled', true);

    $.post({
        url: '/develop/callback',
        data: data,
        dataType: 'json',
        success: function(data) {
            $('#dev-btn-run').html('Run');
            $('#dev-btn-run').prop('disabled', false);

            callback.id = data.id;
            callback.valid = data.valid;
            if(!callback.valid) {
                $('#dev-btn-msg').prop('disabled', false);
                $('#dev-btn-msg').removeClass();
                $('#dev-btn-msg').addClass('btn btn-warning');

                $('#dev-btn-modell').prop('disabled', true);
                $('#dev-btn-modell').html('Error');
                $('#dev-btn-modell').removeClass();
                $('#dev-btn-modell').addClass('btn btn-danger');
            } else {
                $('#dev-btn-msg').prop('disabled', true);
                $('#dev-btn-msg').removeClass();
                $('#dev-btn-msg').addClass('btn btn-default');

                $('#dev-btn-modell').prop('disabled', false);
                $('#dev-btn-modell').html('Details');
                $('#dev-btn-modell').removeClass();
                $('#dev-btn-modell').addClass('btn btn-success');
            }
        }
    });
}

function displayDevelopMessages() {
    if(callback.id == null) return;

    var dialog = bootbox.dialog({
        title: 'Messages',
        size: 'large',
        message: '<p><i class="fa fa-spin fa-spinner"></i> Loading...</p>'
    });

    dialog.init(function() {
        $.ajax({
            dataType: 'json',
            url: '/develop/messages',
            success: function (data) {
                if(data.length == 0) {
                    dialog.find('.bootbox-body').html('<p>No messages</p>');
                    return;
                }

                var content = '<ul>';
                for(var msgNo in data)
                    content += '<li>' + data[msgNo] + '</li>';
                content += '</ul>'

                dialog.find('.bootbox-body').html(content);
            }
        });
    });
}

function deploySearch() {
    var q = $('#deploy-search-field').val();
    if(!q || 0 === q || isEmpty(q)) return;
    var data = { query : q };

    $('#deploy-btn-search').prop('disabled', true);
    $('#deploy-btn-search').html('<i class="fa fa-spin fa-spinner"></i>');


    $.post({
        url: '/deploy/search',
        data: data,
        dataType: 'json',
        success: function(data) {
            file_table.clear().draw();
            deploy_data = data;

            for(var fNo in data) {
                var path = data[fNo].replace(q, '');
                var row = [path];
                file_table.row.add(row);
            }
            file_table.draw();

            $('#deploy-btn-search').prop('disabled', false);
            $('#deploy-btn-search').html('<i class="glyphicon glyphicon-search"></i>');

            $('#deploy-btn-trigger').prop('disabled', false);
            $('#deploy-btn-trigger').html('<i class="glyphicon glyphicon-download"></i> Deploy <span class="badge">' + data.length + '</span>');
        },
        error: function(data) {
            bootbox.alert("Error finding files at the specified location");
            $('#deploy-btn-search').prop('disabled', false);
            $('#deploy-btn-search').html('<i class="glyphicon glyphicon-search"></i>');
        }
    });
}

function deployTrigger() {
    $.post({
        url: '/deploy/trigger',
        success: function(data) { }
    });

    file_table.clear().draw();
    deploy_data = [];
    $('#deploy-search-field').val('');

    $('#deploy-btn-search').prop('disabled', false);
    $('#deploy-btn-search').html('<i class="glyphicon glyphicon-search"></i>');

    $('#deploy-btn-trigger').prop('disabled', true);
    $('#deploy-btn-trigger').html('<i class="glyphicon glyphicon-download"></i> Deploy');
}

function pad(num, size) {
    var s = num+"";
    while (s.length < size) s = "0" + s;
    return s;
}

function formatTimeString(data) {
    var year = data[0];
    var month = pad(data[1], 2);
    var day = pad(data[2], 2);

    if(data.length === 3)
        return year + "-" + month + "-" + day;

    var hour = pad(data[3], 2);
    var min = pad(data[4], 2);
    var sec = data.length === 6 ? pad(data[5], 2) : '00';

    return year + "-" + month + "-" + day + ' ' + hour + ":" + min + ":" + sec;
}