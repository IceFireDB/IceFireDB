$(document).ready(function() {
	$('.mindmap').each(function() {
		MM_FUNCS.drawMindMap(this);
	});
});

var MM_FUNCS = {
	// 将 li 节点转换为 JSON 数据
	li2jsonData: function(liNode) {
		var liData;
		var aNode = liNode.children("a:first");
		if (aNode.length !== 0) {
			liData = {
				"data": {
					"text": aNode.text(),
					"hyperlink": aNode.attr("href")
				}
			};
		} else {
			liData = {
				"data": {
					"text": liNode[0].childNodes[0].nodeValue.trim()
				}
			};
		}
		
		liNode.find("> ul > li").each(function() {
			if (!liData.hasOwnProperty("children")) {
				liData.children = [];
			}
			liData.children.push(MM_FUNCS.li2jsonData($(this)));
		});
		
		return liData;
	},
	// 绘制脑图
	drawMindMap: function(ulParent) {
		var ulElement = $(ulParent).find(">ul:first");
		var mmData = {"root": {}};
		var minder = new kityminder.Minder({
			renderTo: ulParent
		});
		
		mmData.root = MM_FUNCS.li2jsonData(ulElement.children("li:first"));
		minder.importData('json', JSON.stringify(mmData));
		minder.disable();
		minder.execCommand('hand');
		$(ulElement).hide();
	}
};