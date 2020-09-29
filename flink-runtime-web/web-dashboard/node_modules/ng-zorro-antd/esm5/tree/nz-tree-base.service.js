/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { Injectable } from '@angular/core';
import { Subject } from 'rxjs';
import { isNotNil } from '../core/util/check';
import { NzTreeNode } from './nz-tree-node';
import { isCheckDisabled, isInArray } from './nz-tree-util';
var NzTreeBaseService = /** @class */ (function () {
    function NzTreeBaseService() {
        this.DRAG_SIDE_RANGE = 0.25;
        this.DRAG_MIN_GAP = 2;
        this.isCheckStrictly = false;
        this.isMultiple = false;
        this.rootNodes = [];
        this.selectedNodeList = [];
        this.expandedNodeList = [];
        this.checkedNodeList = [];
        this.halfCheckedNodeList = [];
        this.matchedNodeList = [];
        this.triggerEventChange$ = new Subject();
    }
    /**
     * trigger event
     */
    /**
     * trigger event
     * @return {?}
     */
    NzTreeBaseService.prototype.eventTriggerChanged = /**
     * trigger event
     * @return {?}
     */
    function () {
        return this.triggerEventChange$.asObservable();
    };
    /**
     * reset tree nodes will clear default node list
     */
    /**
     * reset tree nodes will clear default node list
     * @param {?} nzNodes
     * @return {?}
     */
    NzTreeBaseService.prototype.initTree = /**
     * reset tree nodes will clear default node list
     * @param {?} nzNodes
     * @return {?}
     */
    function (nzNodes) {
        var _this = this;
        this.rootNodes = nzNodes;
        this.expandedNodeList = [];
        this.selectedNodeList = [];
        this.halfCheckedNodeList = [];
        this.checkedNodeList = [];
        this.matchedNodeList = [];
        // refresh node checked state
        setTimeout((/**
         * @return {?}
         */
        function () {
            _this.refreshCheckState(_this.isCheckStrictly);
        }));
    };
    /**
     * @return {?}
     */
    NzTreeBaseService.prototype.getSelectedNode = /**
     * @return {?}
     */
    function () {
        return this.selectedNode;
    };
    /**
     * get some list
     */
    /**
     * get some list
     * @return {?}
     */
    NzTreeBaseService.prototype.getSelectedNodeList = /**
     * get some list
     * @return {?}
     */
    function () {
        return this.conductNodeState('select');
    };
    /**
     * return checked nodes
     */
    /**
     * return checked nodes
     * @return {?}
     */
    NzTreeBaseService.prototype.getCheckedNodeList = /**
     * return checked nodes
     * @return {?}
     */
    function () {
        return this.conductNodeState('check');
    };
    /**
     * @return {?}
     */
    NzTreeBaseService.prototype.getHalfCheckedNodeList = /**
     * @return {?}
     */
    function () {
        return this.conductNodeState('halfCheck');
    };
    /**
     * return expanded nodes
     */
    /**
     * return expanded nodes
     * @return {?}
     */
    NzTreeBaseService.prototype.getExpandedNodeList = /**
     * return expanded nodes
     * @return {?}
     */
    function () {
        return this.conductNodeState('expand');
    };
    /**
     * return search matched nodes
     */
    /**
     * return search matched nodes
     * @return {?}
     */
    NzTreeBaseService.prototype.getMatchedNodeList = /**
     * return search matched nodes
     * @return {?}
     */
    function () {
        return this.conductNodeState('match');
    };
    // tslint:disable-next-line:no-any
    // tslint:disable-next-line:no-any
    /**
     * @param {?} value
     * @return {?}
     */
    NzTreeBaseService.prototype.isArrayOfNzTreeNode = 
    // tslint:disable-next-line:no-any
    /**
     * @param {?} value
     * @return {?}
     */
    function (value) {
        return value.every((/**
         * @param {?} item
         * @return {?}
         */
        function (item) { return item instanceof NzTreeNode; }));
    };
    /**
     * reset selectedNodeList
     */
    /**
     * reset selectedNodeList
     * @param {?} selectedKeys
     * @param {?} nzNodes
     * @param {?=} isMulti
     * @return {?}
     */
    NzTreeBaseService.prototype.calcSelectedKeys = /**
     * reset selectedNodeList
     * @param {?} selectedKeys
     * @param {?} nzNodes
     * @param {?=} isMulti
     * @return {?}
     */
    function (selectedKeys, nzNodes, isMulti) {
        if (isMulti === void 0) { isMulti = false; }
        /** @type {?} */
        var calc = (/**
         * @param {?} nodes
         * @return {?}
         */
        function (nodes) {
            return nodes.every((/**
             * @param {?} node
             * @return {?}
             */
            function (node) {
                if (isInArray(node.key, selectedKeys)) {
                    node.isSelected = true;
                    if (!isMulti) {
                        // if not support multi select
                        return false;
                    }
                }
                else {
                    node.isSelected = false;
                }
                if (node.children.length > 0) {
                    // Recursion
                    return calc(node.children);
                }
                return true;
            }));
        });
        calc(nzNodes);
    };
    /**
     * reset expandedNodeList
     */
    /**
     * reset expandedNodeList
     * @param {?} expandedKeys
     * @param {?} nzNodes
     * @return {?}
     */
    NzTreeBaseService.prototype.calcExpandedKeys = /**
     * reset expandedNodeList
     * @param {?} expandedKeys
     * @param {?} nzNodes
     * @return {?}
     */
    function (expandedKeys, nzNodes) {
        this.expandedNodeList = [];
        /** @type {?} */
        var calc = (/**
         * @param {?} nodes
         * @return {?}
         */
        function (nodes) {
            nodes.forEach((/**
             * @param {?} node
             * @return {?}
             */
            function (node) {
                if (isInArray(node.key, expandedKeys)) {
                    node.isExpanded = true;
                }
                else {
                    node.isExpanded = false;
                }
                if (node.children.length > 0) {
                    calc(node.children);
                }
            }));
        });
        calc(nzNodes);
    };
    /**
     * reset checkedNodeList
     */
    /**
     * reset checkedNodeList
     * @param {?} checkedKeys
     * @param {?} nzNodes
     * @param {?=} isCheckStrictly
     * @return {?}
     */
    NzTreeBaseService.prototype.calcCheckedKeys = /**
     * reset checkedNodeList
     * @param {?} checkedKeys
     * @param {?} nzNodes
     * @param {?=} isCheckStrictly
     * @return {?}
     */
    function (checkedKeys, nzNodes, isCheckStrictly) {
        if (isCheckStrictly === void 0) { isCheckStrictly = false; }
        this.checkedNodeList = [];
        this.halfCheckedNodeList = [];
        /** @type {?} */
        var calc = (/**
         * @param {?} nodes
         * @return {?}
         */
        function (nodes) {
            nodes.forEach((/**
             * @param {?} node
             * @return {?}
             */
            function (node) {
                if (isInArray(node.key, checkedKeys)) {
                    node.isChecked = true;
                    node.isHalfChecked = false;
                }
                else {
                    node.isChecked = false;
                    node.isHalfChecked = false;
                }
                if (node.children.length > 0) {
                    calc(node.children);
                }
            }));
        });
        calc(nzNodes);
        // controlled state
        this.refreshCheckState(isCheckStrictly);
    };
    /**
     * set drag node
     */
    /**
     * set drag node
     * @param {?} node
     * @return {?}
     */
    NzTreeBaseService.prototype.setSelectedNode = /**
     * set drag node
     * @param {?} node
     * @return {?}
     */
    function (node) {
        this.selectedNode = node;
    };
    /**
     * set node selected status
     */
    /**
     * set node selected status
     * @param {?} node
     * @return {?}
     */
    NzTreeBaseService.prototype.setNodeActive = /**
     * set node selected status
     * @param {?} node
     * @return {?}
     */
    function (node) {
        if (!this.isMultiple && node.isSelected) {
            this.selectedNodeList.forEach((/**
             * @param {?} n
             * @return {?}
             */
            function (n) {
                if (node.key !== n.key) {
                    // reset other nodes
                    n.isSelected = false;
                }
            }));
            // single mode: remove pre node
            this.selectedNodeList = [];
        }
        this.setSelectedNodeList(node, this.isMultiple);
    };
    /**
     * add or remove node to selectedNodeList
     */
    /**
     * add or remove node to selectedNodeList
     * @param {?} node
     * @param {?=} isMultiple
     * @return {?}
     */
    NzTreeBaseService.prototype.setSelectedNodeList = /**
     * add or remove node to selectedNodeList
     * @param {?} node
     * @param {?=} isMultiple
     * @return {?}
     */
    function (node, isMultiple) {
        if (isMultiple === void 0) { isMultiple = false; }
        /** @type {?} */
        var index = this.selectedNodeList.findIndex((/**
         * @param {?} n
         * @return {?}
         */
        function (n) { return node.key === n.key; }));
        if (isMultiple) {
            if (node.isSelected && index === -1) {
                this.selectedNodeList.push(node);
            }
        }
        else {
            if (node.isSelected && index === -1) {
                this.selectedNodeList = [node];
            }
        }
        if (!node.isSelected) {
            this.selectedNodeList = this.selectedNodeList.filter((/**
             * @param {?} n
             * @return {?}
             */
            function (n) { return (n.key !== node.key); }));
        }
    };
    /**
     * merge checked nodes
     */
    /**
     * merge checked nodes
     * @param {?} node
     * @return {?}
     */
    NzTreeBaseService.prototype.setHalfCheckedNodeList = /**
     * merge checked nodes
     * @param {?} node
     * @return {?}
     */
    function (node) {
        /** @type {?} */
        var index = this.halfCheckedNodeList.findIndex((/**
         * @param {?} n
         * @return {?}
         */
        function (n) { return node.key === n.key; }));
        if (node.isHalfChecked && index === -1) {
            this.halfCheckedNodeList.push(node);
        }
        else if (!node.isHalfChecked && index > -1) {
            this.halfCheckedNodeList = this.halfCheckedNodeList.filter((/**
             * @param {?} n
             * @return {?}
             */
            function (n) { return node.key !== n.key; }));
        }
    };
    /**
     * @param {?} node
     * @return {?}
     */
    NzTreeBaseService.prototype.setCheckedNodeList = /**
     * @param {?} node
     * @return {?}
     */
    function (node) {
        /** @type {?} */
        var index = this.checkedNodeList.findIndex((/**
         * @param {?} n
         * @return {?}
         */
        function (n) { return node.key === n.key; }));
        if (node.isChecked && index === -1) {
            this.checkedNodeList.push(node);
        }
        else if (!node.isChecked && index > -1) {
            this.checkedNodeList = this.checkedNodeList.filter((/**
             * @param {?} n
             * @return {?}
             */
            function (n) { return node.key !== n.key; }));
        }
    };
    /**
     * conduct checked/selected/expanded keys
     */
    /**
     * conduct checked/selected/expanded keys
     * @param {?=} type
     * @return {?}
     */
    NzTreeBaseService.prototype.conductNodeState = /**
     * conduct checked/selected/expanded keys
     * @param {?=} type
     * @return {?}
     */
    function (type) {
        var _this = this;
        if (type === void 0) { type = 'check'; }
        /** @type {?} */
        var resultNodesList = [];
        switch (type) {
            case 'select':
                resultNodesList = this.selectedNodeList;
                break;
            case 'expand':
                resultNodesList = this.expandedNodeList;
                break;
            case 'match':
                resultNodesList = this.matchedNodeList;
                break;
            case 'check':
                resultNodesList = this.checkedNodeList;
                /** @type {?} */
                var isIgnore_1 = (/**
                 * @param {?} node
                 * @return {?}
                 */
                function (node) {
                    /** @type {?} */
                    var parentNode = node.getParentNode();
                    if (parentNode) {
                        if (_this.checkedNodeList.findIndex((/**
                         * @param {?} n
                         * @return {?}
                         */
                        function (n) { return n.key === parentNode.key; })) > -1) {
                            return true;
                        }
                        else {
                            return isIgnore_1(parentNode);
                        }
                    }
                    return false;
                });
                // merge checked
                if (!this.isCheckStrictly) {
                    resultNodesList = this.checkedNodeList.filter((/**
                     * @param {?} n
                     * @return {?}
                     */
                    function (n) { return !isIgnore_1(n); }));
                }
                break;
            case 'halfCheck':
                if (!this.isCheckStrictly) {
                    resultNodesList = this.halfCheckedNodeList;
                }
                break;
        }
        return resultNodesList;
    };
    /**
     * set expanded nodes
     */
    /**
     * set expanded nodes
     * @param {?} node
     * @return {?}
     */
    NzTreeBaseService.prototype.setExpandedNodeList = /**
     * set expanded nodes
     * @param {?} node
     * @return {?}
     */
    function (node) {
        if (node.isLeaf) {
            return;
        }
        /** @type {?} */
        var index = this.expandedNodeList.findIndex((/**
         * @param {?} n
         * @return {?}
         */
        function (n) { return node.key === n.key; }));
        if (node.isExpanded && index === -1) {
            this.expandedNodeList.push(node);
        }
        else if (!node.isExpanded && index > -1) {
            this.expandedNodeList = this.expandedNodeList.filter((/**
             * @param {?} n
             * @return {?}
             */
            function (n) { return node.key !== n.key; }));
        }
    };
    /**
     * check state
     * @param node
     */
    /**
     * check state
     * @param {?=} isCheckStrictly
     * @return {?}
     */
    NzTreeBaseService.prototype.refreshCheckState = /**
     * check state
     * @param {?=} isCheckStrictly
     * @return {?}
     */
    function (isCheckStrictly) {
        var _this = this;
        if (isCheckStrictly === void 0) { isCheckStrictly = false; }
        if (isCheckStrictly) {
            return;
        }
        this.checkedNodeList.forEach((/**
         * @param {?} node
         * @return {?}
         */
        function (node) {
            _this.conduct(node);
        }));
    };
    // reset other node checked state based current node
    // reset other node checked state based current node
    /**
     * @param {?} node
     * @return {?}
     */
    NzTreeBaseService.prototype.conduct = 
    // reset other node checked state based current node
    /**
     * @param {?} node
     * @return {?}
     */
    function (node) {
        /** @type {?} */
        var isChecked = node.isChecked;
        if (node) {
            this.conductUp(node);
            this.conductDown(node, isChecked);
        }
    };
    /**
     * 1、children half checked
     * 2、children all checked, parent checked
     * 3、no children checked
     */
    /**
     * 1、children half checked
     * 2、children all checked, parent checked
     * 3、no children checked
     * @param {?} node
     * @return {?}
     */
    NzTreeBaseService.prototype.conductUp = /**
     * 1、children half checked
     * 2、children all checked, parent checked
     * 3、no children checked
     * @param {?} node
     * @return {?}
     */
    function (node) {
        /** @type {?} */
        var parentNode = node.getParentNode();
        // 全禁用节点不选中
        if (parentNode) {
            if (!isCheckDisabled(parentNode)) {
                if (parentNode.children.every((/**
                 * @param {?} child
                 * @return {?}
                 */
                function (child) { return isCheckDisabled(child) || (!child.isHalfChecked && child.isChecked); }))) {
                    parentNode.isChecked = true;
                    parentNode.isHalfChecked = false;
                }
                else if (parentNode.children.some((/**
                 * @param {?} child
                 * @return {?}
                 */
                function (child) { return child.isHalfChecked || child.isChecked; }))) {
                    parentNode.isChecked = false;
                    parentNode.isHalfChecked = true;
                }
                else {
                    parentNode.isChecked = false;
                    parentNode.isHalfChecked = false;
                }
            }
            this.setCheckedNodeList(parentNode);
            this.setHalfCheckedNodeList(parentNode);
            this.conductUp(parentNode);
        }
    };
    /**
     * reset child check state
     */
    /**
     * reset child check state
     * @param {?} node
     * @param {?} value
     * @return {?}
     */
    NzTreeBaseService.prototype.conductDown = /**
     * reset child check state
     * @param {?} node
     * @param {?} value
     * @return {?}
     */
    function (node, value) {
        var _this = this;
        if (!isCheckDisabled(node)) {
            node.isChecked = value;
            node.isHalfChecked = false;
            this.setCheckedNodeList(node);
            this.setHalfCheckedNodeList(node);
            node.children.forEach((/**
             * @param {?} n
             * @return {?}
             */
            function (n) {
                _this.conductDown(n, value);
            }));
        }
    };
    /**
     * search value & expand node
     * should add expandlist
     */
    /**
     * search value & expand node
     * should add expandlist
     * @param {?} value
     * @return {?}
     */
    NzTreeBaseService.prototype.searchExpand = /**
     * search value & expand node
     * should add expandlist
     * @param {?} value
     * @return {?}
     */
    function (value) {
        var _this = this;
        this.matchedNodeList = [];
        /** @type {?} */
        var expandedKeys = [];
        if (!isNotNil(value)) {
            return;
        }
        // to reset expandedNodeList
        /** @type {?} */
        var expandParent = (/**
         * @param {?} n
         * @return {?}
         */
        function (n) {
            // expand parent node
            /** @type {?} */
            var parentNode = n.getParentNode();
            if (parentNode) {
                expandedKeys.push(parentNode.key);
                expandParent(parentNode);
            }
        });
        /** @type {?} */
        var searchChild = (/**
         * @param {?} n
         * @return {?}
         */
        function (n) {
            if (value && n.title.includes(value)) {
                // match the node
                n.isMatched = true;
                _this.matchedNodeList.push(n);
                // expand parentNode
                expandParent(n);
            }
            else {
                n.isMatched = false;
            }
            n.children.forEach((/**
             * @param {?} child
             * @return {?}
             */
            function (child) {
                searchChild(child);
            }));
        });
        this.rootNodes.forEach((/**
         * @param {?} child
         * @return {?}
         */
        function (child) {
            searchChild(child);
        }));
        // expand matched keys
        this.calcExpandedKeys(expandedKeys, this.rootNodes);
    };
    /**
     * flush after delete node
     */
    /**
     * flush after delete node
     * @param {?} nodes
     * @return {?}
     */
    NzTreeBaseService.prototype.afterRemove = /**
     * flush after delete node
     * @param {?} nodes
     * @return {?}
     */
    function (nodes) {
        var _this = this;
        // to reset selectedNodeList & expandedNodeList
        /** @type {?} */
        var loopNode = (/**
         * @param {?} node
         * @return {?}
         */
        function (node) {
            // remove selected node
            _this.selectedNodeList = _this.selectedNodeList.filter((/**
             * @param {?} n
             * @return {?}
             */
            function (n) { return n.key !== node.key; }));
            // remove expanded node
            _this.expandedNodeList = _this.expandedNodeList.filter((/**
             * @param {?} n
             * @return {?}
             */
            function (n) { return n.key !== node.key; }));
            // remove checked node
            _this.checkedNodeList = _this.checkedNodeList.filter((/**
             * @param {?} n
             * @return {?}
             */
            function (n) { return n.key !== node.key; }));
            if (node.children) {
                node.children.forEach((/**
                 * @param {?} child
                 * @return {?}
                 */
                function (child) {
                    loopNode(child);
                }));
            }
        });
        nodes.forEach((/**
         * @param {?} n
         * @return {?}
         */
        function (n) {
            loopNode(n);
        }));
        this.refreshCheckState(this.isCheckStrictly);
    };
    /**
     * drag event
     */
    /**
     * drag event
     * @param {?} node
     * @return {?}
     */
    NzTreeBaseService.prototype.refreshDragNode = /**
     * drag event
     * @param {?} node
     * @return {?}
     */
    function (node) {
        var _this = this;
        if (node.children.length === 0) {
            // until root
            this.conductUp(node);
        }
        else {
            node.children.forEach((/**
             * @param {?} child
             * @return {?}
             */
            function (child) {
                _this.refreshDragNode(child);
            }));
        }
    };
    // reset node level
    // reset node level
    /**
     * @param {?} node
     * @return {?}
     */
    NzTreeBaseService.prototype.resetNodeLevel = 
    // reset node level
    /**
     * @param {?} node
     * @return {?}
     */
    function (node) {
        var e_1, _a;
        /** @type {?} */
        var parentNode = node.getParentNode();
        if (parentNode) {
            node.level = parentNode.level + 1;
        }
        else {
            node.level = 0;
        }
        try {
            for (var _b = tslib_1.__values(node.children), _c = _b.next(); !_c.done; _c = _b.next()) {
                var child = _c.value;
                this.resetNodeLevel(child);
            }
        }
        catch (e_1_1) { e_1 = { error: e_1_1 }; }
        finally {
            try {
                if (_c && !_c.done && (_a = _b.return)) _a.call(_b);
            }
            finally { if (e_1) throw e_1.error; }
        }
    };
    /**
     * @param {?} event
     * @return {?}
     */
    NzTreeBaseService.prototype.calcDropPosition = /**
     * @param {?} event
     * @return {?}
     */
    function (event) {
        var clientY = event.clientY;
        // to fix firefox undefined
        var _a = event.srcElement ? event.srcElement.getBoundingClientRect() : ((/** @type {?} */ (event.target))).getBoundingClientRect(), top = _a.top, bottom = _a.bottom, height = _a.height;
        /** @type {?} */
        var des = Math.max(height * this.DRAG_SIDE_RANGE, this.DRAG_MIN_GAP);
        if (clientY <= top + des) {
            return -1;
        }
        else if (clientY >= bottom - des) {
            return 1;
        }
        return 0;
    };
    /**
     * drop
     * 0: inner -1: pre 1: next
     */
    /**
     * drop
     * 0: inner -1: pre 1: next
     * @param {?} targetNode
     * @param {?=} dragPos
     * @return {?}
     */
    NzTreeBaseService.prototype.dropAndApply = /**
     * drop
     * 0: inner -1: pre 1: next
     * @param {?} targetNode
     * @param {?=} dragPos
     * @return {?}
     */
    function (targetNode, dragPos) {
        var _this = this;
        if (dragPos === void 0) { dragPos = -1; }
        if (!targetNode || dragPos > 1) {
            return;
        }
        /** @type {?} */
        var treeService = targetNode.treeService;
        /** @type {?} */
        var targetParent = targetNode.getParentNode();
        /** @type {?} */
        var isSelectedRootNode = this.selectedNode.getParentNode();
        // remove the dragNode
        if (isSelectedRootNode) {
            isSelectedRootNode.children = isSelectedRootNode.children.filter((/**
             * @param {?} n
             * @return {?}
             */
            function (n) { return n.key !== _this.selectedNode.key; }));
        }
        else {
            this.rootNodes = this.rootNodes.filter((/**
             * @param {?} n
             * @return {?}
             */
            function (n) { return n.key !== _this.selectedNode.key; }));
        }
        switch (dragPos) {
            case 0:
                targetNode.addChildren([this.selectedNode]);
                this.resetNodeLevel(targetNode);
                break;
            case -1:
            case 1:
                /** @type {?} */
                var tIndex = dragPos === 1 ? 1 : 0;
                if (targetParent) {
                    targetParent.addChildren([this.selectedNode], targetParent.children.indexOf(targetNode) + tIndex);
                    /** @type {?} */
                    var parentNode = this.selectedNode.getParentNode();
                    if (parentNode) {
                        this.resetNodeLevel(parentNode);
                    }
                }
                else {
                    /** @type {?} */
                    var targetIndex = this.rootNodes.indexOf(targetNode) + tIndex;
                    // 根节点插入
                    this.rootNodes.splice(targetIndex, 0, this.selectedNode);
                    this.rootNodes[targetIndex].parentNode = null;
                    this.rootNodes[targetIndex].level = 0;
                }
                break;
        }
        // flush all nodes
        this.rootNodes.forEach((/**
         * @param {?} child
         * @return {?}
         */
        function (child) {
            if (!child.treeService) {
                child.service = treeService;
            }
            _this.refreshDragNode(child);
        }));
    };
    /**
     * emit Structure
     * eventName
     * node
     * event: MouseEvent / DragEvent
     * dragNode
     */
    /**
     * emit Structure
     * eventName
     * node
     * event: MouseEvent / DragEvent
     * dragNode
     * @param {?} eventName
     * @param {?} node
     * @param {?} event
     * @return {?}
     */
    NzTreeBaseService.prototype.formatEvent = /**
     * emit Structure
     * eventName
     * node
     * event: MouseEvent / DragEvent
     * dragNode
     * @param {?} eventName
     * @param {?} node
     * @param {?} event
     * @return {?}
     */
    function (eventName, node, event) {
        /** @type {?} */
        var emitStructure = {
            'eventName': eventName,
            'node': node,
            'event': event
        };
        switch (eventName) {
            case 'dragstart':
            case 'dragenter':
            case 'dragover':
            case 'dragleave':
            case 'drop':
            case 'dragend':
                Object.assign(emitStructure, { 'dragNode': this.getSelectedNode() });
                break;
            case 'click':
            case 'dblclick':
                Object.assign(emitStructure, { 'selectedKeys': this.selectedNodeList });
                Object.assign(emitStructure, { 'nodes': this.selectedNodeList });
                Object.assign(emitStructure, { 'keys': this.selectedNodeList.map((/**
                     * @param {?} n
                     * @return {?}
                     */
                    function (n) { return n.key; })) });
                break;
            case 'check':
                /** @type {?} */
                var checkedNodeList = this.getCheckedNodeList();
                Object.assign(emitStructure, { 'checkedKeys': checkedNodeList });
                Object.assign(emitStructure, { 'nodes': checkedNodeList });
                Object.assign(emitStructure, { 'keys': checkedNodeList.map((/**
                     * @param {?} n
                     * @return {?}
                     */
                    function (n) { return n.key; })) });
                break;
            case 'search':
                Object.assign(emitStructure, { 'matchedKeys': this.getMatchedNodeList() });
                Object.assign(emitStructure, { 'nodes': this.getMatchedNodeList() });
                Object.assign(emitStructure, { 'keys': this.getMatchedNodeList().map((/**
                     * @param {?} n
                     * @return {?}
                     */
                    function (n) { return n.key; })) });
                break;
            case 'expand':
                Object.assign(emitStructure, { 'nodes': this.expandedNodeList });
                Object.assign(emitStructure, { 'keys': this.expandedNodeList.map((/**
                     * @param {?} n
                     * @return {?}
                     */
                    function (n) { return n.key; })) });
                break;
        }
        return emitStructure;
    };
    /**
     * @return {?}
     */
    NzTreeBaseService.prototype.ngOnDestroy = /**
     * @return {?}
     */
    function () {
        this.triggerEventChange$.complete();
    };
    NzTreeBaseService.decorators = [
        { type: Injectable }
    ];
    return NzTreeBaseService;
}());
export { NzTreeBaseService };
if (false) {
    /** @type {?} */
    NzTreeBaseService.prototype.DRAG_SIDE_RANGE;
    /** @type {?} */
    NzTreeBaseService.prototype.DRAG_MIN_GAP;
    /** @type {?} */
    NzTreeBaseService.prototype.isCheckStrictly;
    /** @type {?} */
    NzTreeBaseService.prototype.isMultiple;
    /** @type {?} */
    NzTreeBaseService.prototype.selectedNode;
    /** @type {?} */
    NzTreeBaseService.prototype.rootNodes;
    /** @type {?} */
    NzTreeBaseService.prototype.selectedNodeList;
    /** @type {?} */
    NzTreeBaseService.prototype.expandedNodeList;
    /** @type {?} */
    NzTreeBaseService.prototype.checkedNodeList;
    /** @type {?} */
    NzTreeBaseService.prototype.halfCheckedNodeList;
    /** @type {?} */
    NzTreeBaseService.prototype.matchedNodeList;
    /** @type {?} */
    NzTreeBaseService.prototype.triggerEventChange$;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotdHJlZS1iYXNlLnNlcnZpY2UuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsidHJlZS9uei10cmVlLWJhc2Uuc2VydmljZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFBRSxVQUFVLEVBQWEsTUFBTSxlQUFlLENBQUM7QUFDdEQsT0FBTyxFQUFjLE9BQU8sRUFBRSxNQUFNLE1BQU0sQ0FBQztBQUMzQyxPQUFPLEVBQUUsUUFBUSxFQUFFLE1BQU0sb0JBQW9CLENBQUM7QUFFOUMsT0FBTyxFQUFFLFVBQVUsRUFBRSxNQUFNLGdCQUFnQixDQUFDO0FBQzVDLE9BQU8sRUFBRSxlQUFlLEVBQUUsU0FBUyxFQUFFLE1BQU0sZ0JBQWdCLENBQUM7QUFFNUQ7SUFBQTtRQUVFLG9CQUFlLEdBQUcsSUFBSSxDQUFDO1FBQ3ZCLGlCQUFZLEdBQUcsQ0FBQyxDQUFDO1FBRWpCLG9CQUFlLEdBQVksS0FBSyxDQUFDO1FBQ2pDLGVBQVUsR0FBWSxLQUFLLENBQUM7UUFFNUIsY0FBUyxHQUFpQixFQUFFLENBQUM7UUFDN0IscUJBQWdCLEdBQWlCLEVBQUUsQ0FBQztRQUNwQyxxQkFBZ0IsR0FBaUIsRUFBRSxDQUFDO1FBQ3BDLG9CQUFlLEdBQWlCLEVBQUUsQ0FBQztRQUNuQyx3QkFBbUIsR0FBaUIsRUFBRSxDQUFDO1FBQ3ZDLG9CQUFlLEdBQWlCLEVBQUUsQ0FBQztRQUNuQyx3QkFBbUIsR0FBRyxJQUFJLE9BQU8sRUFBcUIsQ0FBQztJQStnQnpELENBQUM7SUE3Z0JDOztPQUVHOzs7OztJQUNILCtDQUFtQjs7OztJQUFuQjtRQUNFLE9BQU8sSUFBSSxDQUFDLG1CQUFtQixDQUFDLFlBQVksRUFBRSxDQUFDO0lBQ2pELENBQUM7SUFFRDs7T0FFRzs7Ozs7O0lBQ0gsb0NBQVE7Ozs7O0lBQVIsVUFBUyxPQUFxQjtRQUE5QixpQkFXQztRQVZDLElBQUksQ0FBQyxTQUFTLEdBQUcsT0FBTyxDQUFDO1FBQ3pCLElBQUksQ0FBQyxnQkFBZ0IsR0FBRyxFQUFFLENBQUM7UUFDM0IsSUFBSSxDQUFDLGdCQUFnQixHQUFHLEVBQUUsQ0FBQztRQUMzQixJQUFJLENBQUMsbUJBQW1CLEdBQUcsRUFBRSxDQUFDO1FBQzlCLElBQUksQ0FBQyxlQUFlLEdBQUcsRUFBRSxDQUFDO1FBQzFCLElBQUksQ0FBQyxlQUFlLEdBQUcsRUFBRSxDQUFDO1FBQzFCLDZCQUE2QjtRQUM3QixVQUFVOzs7UUFBQztZQUNULEtBQUksQ0FBQyxpQkFBaUIsQ0FBQyxLQUFJLENBQUMsZUFBZSxDQUFDLENBQUM7UUFDL0MsQ0FBQyxFQUFDLENBQUM7SUFDTCxDQUFDOzs7O0lBRUQsMkNBQWU7OztJQUFmO1FBQ0UsT0FBTyxJQUFJLENBQUMsWUFBWSxDQUFDO0lBQzNCLENBQUM7SUFFRDs7T0FFRzs7Ozs7SUFDSCwrQ0FBbUI7Ozs7SUFBbkI7UUFDRSxPQUFPLElBQUksQ0FBQyxnQkFBZ0IsQ0FBQyxRQUFRLENBQUMsQ0FBQztJQUN6QyxDQUFDO0lBRUQ7O09BRUc7Ozs7O0lBQ0gsOENBQWtCOzs7O0lBQWxCO1FBQ0UsT0FBTyxJQUFJLENBQUMsZ0JBQWdCLENBQUMsT0FBTyxDQUFDLENBQUM7SUFDeEMsQ0FBQzs7OztJQUVELGtEQUFzQjs7O0lBQXRCO1FBQ0UsT0FBTyxJQUFJLENBQUMsZ0JBQWdCLENBQUMsV0FBVyxDQUFDLENBQUM7SUFDNUMsQ0FBQztJQUVEOztPQUVHOzs7OztJQUNILCtDQUFtQjs7OztJQUFuQjtRQUNFLE9BQU8sSUFBSSxDQUFDLGdCQUFnQixDQUFDLFFBQVEsQ0FBQyxDQUFDO0lBQ3pDLENBQUM7SUFFRDs7T0FFRzs7Ozs7SUFDSCw4Q0FBa0I7Ozs7SUFBbEI7UUFDRSxPQUFPLElBQUksQ0FBQyxnQkFBZ0IsQ0FBQyxPQUFPLENBQUMsQ0FBQztJQUN4QyxDQUFDO0lBRUQsa0NBQWtDOzs7Ozs7SUFDbEMsK0NBQW1COzs7Ozs7SUFBbkIsVUFBb0IsS0FBWTtRQUM5QixPQUFPLEtBQUssQ0FBQyxLQUFLOzs7O1FBQUMsVUFBQSxJQUFJLElBQUksT0FBQSxJQUFJLFlBQVksVUFBVSxFQUExQixDQUEwQixFQUFDLENBQUM7SUFDekQsQ0FBQztJQUVEOztPQUVHOzs7Ozs7OztJQUNILDRDQUFnQjs7Ozs7OztJQUFoQixVQUFpQixZQUFzQixFQUFFLE9BQXFCLEVBQUUsT0FBd0I7UUFBeEIsd0JBQUEsRUFBQSxlQUF3Qjs7WUFDaEYsSUFBSTs7OztRQUFHLFVBQUMsS0FBbUI7WUFDL0IsT0FBTyxLQUFLLENBQUMsS0FBSzs7OztZQUFDLFVBQUEsSUFBSTtnQkFDckIsSUFBSSxTQUFTLENBQUMsSUFBSSxDQUFDLEdBQUcsRUFBRSxZQUFZLENBQUMsRUFBRTtvQkFDckMsSUFBSSxDQUFDLFVBQVUsR0FBRyxJQUFJLENBQUM7b0JBQ3ZCLElBQUksQ0FBQyxPQUFPLEVBQUU7d0JBQ1osOEJBQThCO3dCQUM5QixPQUFPLEtBQUssQ0FBQztxQkFDZDtpQkFDRjtxQkFBTTtvQkFDTCxJQUFJLENBQUMsVUFBVSxHQUFHLEtBQUssQ0FBQztpQkFDekI7Z0JBQ0QsSUFBSSxJQUFJLENBQUMsUUFBUSxDQUFDLE1BQU0sR0FBRyxDQUFDLEVBQUU7b0JBQzVCLFlBQVk7b0JBQ1osT0FBTyxJQUFJLENBQUMsSUFBSSxDQUFDLFFBQVEsQ0FBQyxDQUFDO2lCQUM1QjtnQkFDRCxPQUFPLElBQUksQ0FBQztZQUNkLENBQUMsRUFBQyxDQUFDO1FBQ0wsQ0FBQyxDQUFBO1FBQ0QsSUFBSSxDQUFDLE9BQU8sQ0FBQyxDQUFDO0lBQ2hCLENBQUM7SUFFRDs7T0FFRzs7Ozs7OztJQUNILDRDQUFnQjs7Ozs7O0lBQWhCLFVBQWlCLFlBQXNCLEVBQUUsT0FBcUI7UUFDNUQsSUFBSSxDQUFDLGdCQUFnQixHQUFHLEVBQUUsQ0FBQzs7WUFDckIsSUFBSTs7OztRQUFHLFVBQUMsS0FBbUI7WUFDL0IsS0FBSyxDQUFDLE9BQU87Ozs7WUFBQyxVQUFBLElBQUk7Z0JBQ2hCLElBQUksU0FBUyxDQUFDLElBQUksQ0FBQyxHQUFHLEVBQUUsWUFBWSxDQUFDLEVBQUU7b0JBQ3JDLElBQUksQ0FBQyxVQUFVLEdBQUcsSUFBSSxDQUFDO2lCQUN4QjtxQkFBTTtvQkFDTCxJQUFJLENBQUMsVUFBVSxHQUFHLEtBQUssQ0FBQztpQkFDekI7Z0JBQ0QsSUFBSSxJQUFJLENBQUMsUUFBUSxDQUFDLE1BQU0sR0FBRyxDQUFDLEVBQUU7b0JBQzVCLElBQUksQ0FBQyxJQUFJLENBQUMsUUFBUSxDQUFDLENBQUM7aUJBQ3JCO1lBQ0gsQ0FBQyxFQUFDLENBQUM7UUFDTCxDQUFDLENBQUE7UUFDRCxJQUFJLENBQUMsT0FBTyxDQUFDLENBQUM7SUFDaEIsQ0FBQztJQUVEOztPQUVHOzs7Ozs7OztJQUNILDJDQUFlOzs7Ozs7O0lBQWYsVUFBZ0IsV0FBcUIsRUFBRSxPQUFxQixFQUFFLGVBQWdDO1FBQWhDLGdDQUFBLEVBQUEsdUJBQWdDO1FBQzVGLElBQUksQ0FBQyxlQUFlLEdBQUcsRUFBRSxDQUFDO1FBQzFCLElBQUksQ0FBQyxtQkFBbUIsR0FBRyxFQUFFLENBQUM7O1lBQ3hCLElBQUk7Ozs7UUFBRyxVQUFDLEtBQW1CO1lBQy9CLEtBQUssQ0FBQyxPQUFPOzs7O1lBQUMsVUFBQSxJQUFJO2dCQUNoQixJQUFJLFNBQVMsQ0FBQyxJQUFJLENBQUMsR0FBRyxFQUFFLFdBQVcsQ0FBQyxFQUFFO29CQUNwQyxJQUFJLENBQUMsU0FBUyxHQUFHLElBQUksQ0FBQztvQkFDdEIsSUFBSSxDQUFDLGFBQWEsR0FBRyxLQUFLLENBQUM7aUJBQzVCO3FCQUFNO29CQUNMLElBQUksQ0FBQyxTQUFTLEdBQUcsS0FBSyxDQUFDO29CQUN2QixJQUFJLENBQUMsYUFBYSxHQUFHLEtBQUssQ0FBQztpQkFDNUI7Z0JBQ0QsSUFBSSxJQUFJLENBQUMsUUFBUSxDQUFDLE1BQU0sR0FBRyxDQUFDLEVBQUU7b0JBQzVCLElBQUksQ0FBQyxJQUFJLENBQUMsUUFBUSxDQUFDLENBQUM7aUJBQ3JCO1lBQ0gsQ0FBQyxFQUFDLENBQUM7UUFDTCxDQUFDLENBQUE7UUFDRCxJQUFJLENBQUMsT0FBTyxDQUFDLENBQUM7UUFDZCxtQkFBbUI7UUFDbkIsSUFBSSxDQUFDLGlCQUFpQixDQUFDLGVBQWUsQ0FBQyxDQUFDO0lBQzFDLENBQUM7SUFFRDs7T0FFRzs7Ozs7O0lBQ0gsMkNBQWU7Ozs7O0lBQWYsVUFBZ0IsSUFBZ0I7UUFDOUIsSUFBSSxDQUFDLFlBQVksR0FBRyxJQUFJLENBQUM7SUFDM0IsQ0FBQztJQUVEOztPQUVHOzs7Ozs7SUFDSCx5Q0FBYTs7Ozs7SUFBYixVQUFjLElBQWdCO1FBQzVCLElBQUksQ0FBQyxJQUFJLENBQUMsVUFBVSxJQUFJLElBQUksQ0FBQyxVQUFVLEVBQUU7WUFDdkMsSUFBSSxDQUFDLGdCQUFnQixDQUFDLE9BQU87Ozs7WUFBQyxVQUFBLENBQUM7Z0JBQzdCLElBQUksSUFBSSxDQUFDLEdBQUcsS0FBSyxDQUFDLENBQUMsR0FBRyxFQUFFO29CQUN0QixvQkFBb0I7b0JBQ3BCLENBQUMsQ0FBQyxVQUFVLEdBQUcsS0FBSyxDQUFDO2lCQUN0QjtZQUNILENBQUMsRUFBQyxDQUFDO1lBQ0gsK0JBQStCO1lBQy9CLElBQUksQ0FBQyxnQkFBZ0IsR0FBRyxFQUFFLENBQUM7U0FDNUI7UUFDRCxJQUFJLENBQUMsbUJBQW1CLENBQUMsSUFBSSxFQUFFLElBQUksQ0FBQyxVQUFVLENBQUMsQ0FBQztJQUNsRCxDQUFDO0lBRUQ7O09BRUc7Ozs7Ozs7SUFDSCwrQ0FBbUI7Ozs7OztJQUFuQixVQUFvQixJQUFnQixFQUFFLFVBQTJCO1FBQTNCLDJCQUFBLEVBQUEsa0JBQTJCOztZQUN6RCxLQUFLLEdBQUcsSUFBSSxDQUFDLGdCQUFnQixDQUFDLFNBQVM7Ozs7UUFBQyxVQUFBLENBQUMsSUFBSSxPQUFBLElBQUksQ0FBQyxHQUFHLEtBQUssQ0FBQyxDQUFDLEdBQUcsRUFBbEIsQ0FBa0IsRUFBQztRQUN0RSxJQUFJLFVBQVUsRUFBRTtZQUNkLElBQUksSUFBSSxDQUFDLFVBQVUsSUFBSSxLQUFLLEtBQUssQ0FBQyxDQUFDLEVBQUU7Z0JBQ25DLElBQUksQ0FBQyxnQkFBZ0IsQ0FBQyxJQUFJLENBQUMsSUFBSSxDQUFDLENBQUM7YUFDbEM7U0FDRjthQUFNO1lBQ0wsSUFBSSxJQUFJLENBQUMsVUFBVSxJQUFJLEtBQUssS0FBSyxDQUFDLENBQUMsRUFBRTtnQkFDbkMsSUFBSSxDQUFDLGdCQUFnQixHQUFHLENBQUUsSUFBSSxDQUFFLENBQUM7YUFDbEM7U0FDRjtRQUNELElBQUksQ0FBQyxJQUFJLENBQUMsVUFBVSxFQUFFO1lBQ3BCLElBQUksQ0FBQyxnQkFBZ0IsR0FBRyxJQUFJLENBQUMsZ0JBQWdCLENBQUMsTUFBTTs7OztZQUFDLFVBQUEsQ0FBQyxJQUFJLE9BQUEsQ0FBQyxDQUFDLENBQUMsR0FBRyxLQUFLLElBQUksQ0FBQyxHQUFHLENBQUMsRUFBcEIsQ0FBb0IsRUFBQyxDQUFDO1NBQ2pGO0lBQ0gsQ0FBQztJQUVEOztPQUVHOzs7Ozs7SUFDSCxrREFBc0I7Ozs7O0lBQXRCLFVBQXVCLElBQWdCOztZQUMvQixLQUFLLEdBQUcsSUFBSSxDQUFDLG1CQUFtQixDQUFDLFNBQVM7Ozs7UUFBQyxVQUFBLENBQUMsSUFBSSxPQUFBLElBQUksQ0FBQyxHQUFHLEtBQUssQ0FBQyxDQUFDLEdBQUcsRUFBbEIsQ0FBa0IsRUFBQztRQUN6RSxJQUFJLElBQUksQ0FBQyxhQUFhLElBQUksS0FBSyxLQUFLLENBQUMsQ0FBQyxFQUFFO1lBQ3RDLElBQUksQ0FBQyxtQkFBbUIsQ0FBQyxJQUFJLENBQUMsSUFBSSxDQUFDLENBQUM7U0FDckM7YUFBTSxJQUFJLENBQUMsSUFBSSxDQUFDLGFBQWEsSUFBSSxLQUFLLEdBQUcsQ0FBQyxDQUFDLEVBQUU7WUFDNUMsSUFBSSxDQUFDLG1CQUFtQixHQUFHLElBQUksQ0FBQyxtQkFBbUIsQ0FBQyxNQUFNOzs7O1lBQUMsVUFBQSxDQUFDLElBQUksT0FBQSxJQUFJLENBQUMsR0FBRyxLQUFLLENBQUMsQ0FBQyxHQUFHLEVBQWxCLENBQWtCLEVBQUMsQ0FBQztTQUNyRjtJQUNILENBQUM7Ozs7O0lBRUQsOENBQWtCOzs7O0lBQWxCLFVBQW1CLElBQWdCOztZQUMzQixLQUFLLEdBQUcsSUFBSSxDQUFDLGVBQWUsQ0FBQyxTQUFTOzs7O1FBQUMsVUFBQSxDQUFDLElBQUksT0FBQSxJQUFJLENBQUMsR0FBRyxLQUFLLENBQUMsQ0FBQyxHQUFHLEVBQWxCLENBQWtCLEVBQUM7UUFDckUsSUFBSSxJQUFJLENBQUMsU0FBUyxJQUFJLEtBQUssS0FBSyxDQUFDLENBQUMsRUFBRTtZQUNsQyxJQUFJLENBQUMsZUFBZSxDQUFDLElBQUksQ0FBQyxJQUFJLENBQUMsQ0FBQztTQUNqQzthQUFNLElBQUksQ0FBQyxJQUFJLENBQUMsU0FBUyxJQUFJLEtBQUssR0FBRyxDQUFDLENBQUMsRUFBRTtZQUN4QyxJQUFJLENBQUMsZUFBZSxHQUFHLElBQUksQ0FBQyxlQUFlLENBQUMsTUFBTTs7OztZQUFDLFVBQUEsQ0FBQyxJQUFJLE9BQUEsSUFBSSxDQUFDLEdBQUcsS0FBSyxDQUFDLENBQUMsR0FBRyxFQUFsQixDQUFrQixFQUFDLENBQUM7U0FDN0U7SUFDSCxDQUFDO0lBRUQ7O09BRUc7Ozs7OztJQUNILDRDQUFnQjs7Ozs7SUFBaEIsVUFBaUIsSUFBc0I7UUFBdkMsaUJBcUNDO1FBckNnQixxQkFBQSxFQUFBLGNBQXNCOztZQUNqQyxlQUFlLEdBQWlCLEVBQUU7UUFDdEMsUUFBUSxJQUFJLEVBQUU7WUFDWixLQUFLLFFBQVE7Z0JBQ1gsZUFBZSxHQUFHLElBQUksQ0FBQyxnQkFBZ0IsQ0FBQztnQkFDeEMsTUFBTTtZQUNSLEtBQUssUUFBUTtnQkFDWCxlQUFlLEdBQUcsSUFBSSxDQUFDLGdCQUFnQixDQUFDO2dCQUN4QyxNQUFNO1lBQ1IsS0FBSyxPQUFPO2dCQUNWLGVBQWUsR0FBRyxJQUFJLENBQUMsZUFBZSxDQUFDO2dCQUN2QyxNQUFNO1lBQ1IsS0FBSyxPQUFPO2dCQUNWLGVBQWUsR0FBRyxJQUFJLENBQUMsZUFBZSxDQUFDOztvQkFDakMsVUFBUTs7OztnQkFBRyxVQUFDLElBQWdCOzt3QkFDMUIsVUFBVSxHQUFHLElBQUksQ0FBQyxhQUFhLEVBQUU7b0JBQ3ZDLElBQUksVUFBVSxFQUFFO3dCQUNkLElBQUksS0FBSSxDQUFDLGVBQWUsQ0FBQyxTQUFTOzs7O3dCQUFDLFVBQUEsQ0FBQyxJQUFJLE9BQUEsQ0FBQyxDQUFDLEdBQUcsS0FBSyxVQUFVLENBQUMsR0FBRyxFQUF4QixDQUF3QixFQUFDLEdBQUcsQ0FBQyxDQUFDLEVBQUU7NEJBQ3RFLE9BQU8sSUFBSSxDQUFDO3lCQUNiOzZCQUFNOzRCQUNMLE9BQU8sVUFBUSxDQUFDLFVBQVUsQ0FBQyxDQUFDO3lCQUM3QjtxQkFDRjtvQkFDRCxPQUFPLEtBQUssQ0FBQztnQkFDZixDQUFDLENBQUE7Z0JBQ0QsZ0JBQWdCO2dCQUNoQixJQUFJLENBQUMsSUFBSSxDQUFDLGVBQWUsRUFBRTtvQkFDekIsZUFBZSxHQUFHLElBQUksQ0FBQyxlQUFlLENBQUMsTUFBTTs7OztvQkFBQyxVQUFBLENBQUMsSUFBSSxPQUFBLENBQUMsVUFBUSxDQUFDLENBQUMsQ0FBQyxFQUFaLENBQVksRUFBQyxDQUFDO2lCQUNsRTtnQkFDRCxNQUFNO1lBQ1IsS0FBSyxXQUFXO2dCQUNkLElBQUksQ0FBQyxJQUFJLENBQUMsZUFBZSxFQUFFO29CQUN6QixlQUFlLEdBQUcsSUFBSSxDQUFDLG1CQUFtQixDQUFDO2lCQUM1QztnQkFDRCxNQUFNO1NBQ1Q7UUFDRCxPQUFPLGVBQWUsQ0FBQztJQUN6QixDQUFDO0lBRUQ7O09BRUc7Ozs7OztJQUNILCtDQUFtQjs7Ozs7SUFBbkIsVUFBb0IsSUFBZ0I7UUFDbEMsSUFBSSxJQUFJLENBQUMsTUFBTSxFQUFFO1lBQ2YsT0FBTztTQUNSOztZQUNLLEtBQUssR0FBRyxJQUFJLENBQUMsZ0JBQWdCLENBQUMsU0FBUzs7OztRQUFDLFVBQUEsQ0FBQyxJQUFJLE9BQUEsSUFBSSxDQUFDLEdBQUcsS0FBSyxDQUFDLENBQUMsR0FBRyxFQUFsQixDQUFrQixFQUFDO1FBQ3RFLElBQUksSUFBSSxDQUFDLFVBQVUsSUFBSSxLQUFLLEtBQUssQ0FBQyxDQUFDLEVBQUU7WUFDbkMsSUFBSSxDQUFDLGdCQUFnQixDQUFDLElBQUksQ0FBQyxJQUFJLENBQUMsQ0FBQztTQUNsQzthQUFNLElBQUksQ0FBQyxJQUFJLENBQUMsVUFBVSxJQUFJLEtBQUssR0FBRyxDQUFDLENBQUMsRUFBRTtZQUN6QyxJQUFJLENBQUMsZ0JBQWdCLEdBQUcsSUFBSSxDQUFDLGdCQUFnQixDQUFDLE1BQU07Ozs7WUFBQyxVQUFBLENBQUMsSUFBSSxPQUFBLElBQUksQ0FBQyxHQUFHLEtBQUssQ0FBQyxDQUFDLEdBQUcsRUFBbEIsQ0FBa0IsRUFBQyxDQUFDO1NBQy9FO0lBQ0gsQ0FBQztJQUVEOzs7T0FHRzs7Ozs7O0lBQ0gsNkNBQWlCOzs7OztJQUFqQixVQUFrQixlQUFnQztRQUFsRCxpQkFPQztRQVBpQixnQ0FBQSxFQUFBLHVCQUFnQztRQUNoRCxJQUFJLGVBQWUsRUFBRTtZQUNuQixPQUFPO1NBQ1I7UUFDRCxJQUFJLENBQUMsZUFBZSxDQUFDLE9BQU87Ozs7UUFBQyxVQUFBLElBQUk7WUFDL0IsS0FBSSxDQUFDLE9BQU8sQ0FBQyxJQUFJLENBQUMsQ0FBQztRQUNyQixDQUFDLEVBQUMsQ0FBQztJQUNMLENBQUM7SUFFRCxvREFBb0Q7Ozs7OztJQUNwRCxtQ0FBTzs7Ozs7O0lBQVAsVUFBUSxJQUFnQjs7WUFDaEIsU0FBUyxHQUFHLElBQUksQ0FBQyxTQUFTO1FBQ2hDLElBQUksSUFBSSxFQUFFO1lBQ1IsSUFBSSxDQUFDLFNBQVMsQ0FBQyxJQUFJLENBQUMsQ0FBQztZQUNyQixJQUFJLENBQUMsV0FBVyxDQUFDLElBQUksRUFBRSxTQUFTLENBQUMsQ0FBQztTQUNuQztJQUNILENBQUM7SUFFRDs7OztPQUlHOzs7Ozs7OztJQUNILHFDQUFTOzs7Ozs7O0lBQVQsVUFBVSxJQUFnQjs7WUFDbEIsVUFBVSxHQUFHLElBQUksQ0FBQyxhQUFhLEVBQUU7UUFDdkMsV0FBVztRQUNYLElBQUksVUFBVSxFQUFFO1lBQ2QsSUFBSSxDQUFDLGVBQWUsQ0FBQyxVQUFVLENBQUMsRUFBRTtnQkFDaEMsSUFBSSxVQUFVLENBQUMsUUFBUSxDQUFDLEtBQUs7Ozs7Z0JBQUMsVUFBQSxLQUFLLElBQUksT0FBQSxlQUFlLENBQUMsS0FBSyxDQUFDLElBQUksQ0FBQyxDQUFDLEtBQUssQ0FBQyxhQUFhLElBQUksS0FBSyxDQUFDLFNBQVMsQ0FBQyxFQUFuRSxDQUFtRSxFQUFDLEVBQUU7b0JBQzNHLFVBQVUsQ0FBQyxTQUFTLEdBQUcsSUFBSSxDQUFDO29CQUM1QixVQUFVLENBQUMsYUFBYSxHQUFHLEtBQUssQ0FBQztpQkFDbEM7cUJBQU0sSUFBSSxVQUFVLENBQUMsUUFBUSxDQUFDLElBQUk7Ozs7Z0JBQUMsVUFBQSxLQUFLLElBQUksT0FBQSxLQUFLLENBQUMsYUFBYSxJQUFJLEtBQUssQ0FBQyxTQUFTLEVBQXRDLENBQXNDLEVBQUMsRUFBRTtvQkFDcEYsVUFBVSxDQUFDLFNBQVMsR0FBRyxLQUFLLENBQUM7b0JBQzdCLFVBQVUsQ0FBQyxhQUFhLEdBQUcsSUFBSSxDQUFDO2lCQUNqQztxQkFBTTtvQkFDTCxVQUFVLENBQUMsU0FBUyxHQUFHLEtBQUssQ0FBQztvQkFDN0IsVUFBVSxDQUFDLGFBQWEsR0FBRyxLQUFLLENBQUM7aUJBQ2xDO2FBQ0Y7WUFDRCxJQUFJLENBQUMsa0JBQWtCLENBQUMsVUFBVSxDQUFDLENBQUM7WUFDcEMsSUFBSSxDQUFDLHNCQUFzQixDQUFDLFVBQVUsQ0FBQyxDQUFDO1lBQ3hDLElBQUksQ0FBQyxTQUFTLENBQUMsVUFBVSxDQUFDLENBQUM7U0FDNUI7SUFDSCxDQUFDO0lBRUQ7O09BRUc7Ozs7Ozs7SUFDSCx1Q0FBVzs7Ozs7O0lBQVgsVUFBWSxJQUFnQixFQUFFLEtBQWM7UUFBNUMsaUJBVUM7UUFUQyxJQUFJLENBQUMsZUFBZSxDQUFDLElBQUksQ0FBQyxFQUFFO1lBQzFCLElBQUksQ0FBQyxTQUFTLEdBQUcsS0FBSyxDQUFDO1lBQ3ZCLElBQUksQ0FBQyxhQUFhLEdBQUcsS0FBSyxDQUFDO1lBQzNCLElBQUksQ0FBQyxrQkFBa0IsQ0FBQyxJQUFJLENBQUMsQ0FBQztZQUM5QixJQUFJLENBQUMsc0JBQXNCLENBQUMsSUFBSSxDQUFDLENBQUM7WUFDbEMsSUFBSSxDQUFDLFFBQVEsQ0FBQyxPQUFPOzs7O1lBQUMsVUFBQSxDQUFDO2dCQUNyQixLQUFJLENBQUMsV0FBVyxDQUFDLENBQUMsRUFBRSxLQUFLLENBQUMsQ0FBQztZQUM3QixDQUFDLEVBQUMsQ0FBQztTQUNKO0lBQ0gsQ0FBQztJQUVEOzs7T0FHRzs7Ozs7OztJQUNILHdDQUFZOzs7Ozs7SUFBWixVQUFhLEtBQWE7UUFBMUIsaUJBa0NDO1FBakNDLElBQUksQ0FBQyxlQUFlLEdBQUcsRUFBRSxDQUFDOztZQUNwQixZQUFZLEdBQWEsRUFBRTtRQUNqQyxJQUFJLENBQUMsUUFBUSxDQUFDLEtBQUssQ0FBQyxFQUFFO1lBQ3BCLE9BQU87U0FDUjs7O1lBRUssWUFBWTs7OztRQUFHLFVBQUMsQ0FBYTs7O2dCQUUzQixVQUFVLEdBQUcsQ0FBQyxDQUFDLGFBQWEsRUFBRTtZQUNwQyxJQUFJLFVBQVUsRUFBRTtnQkFDZCxZQUFZLENBQUMsSUFBSSxDQUFDLFVBQVUsQ0FBQyxHQUFHLENBQUMsQ0FBQztnQkFDbEMsWUFBWSxDQUFDLFVBQVUsQ0FBQyxDQUFDO2FBQzFCO1FBQ0gsQ0FBQyxDQUFBOztZQUNLLFdBQVc7Ozs7UUFBRyxVQUFDLENBQWE7WUFDaEMsSUFBSSxLQUFLLElBQUksQ0FBQyxDQUFDLEtBQUssQ0FBQyxRQUFRLENBQUMsS0FBSyxDQUFDLEVBQUU7Z0JBQ3BDLGlCQUFpQjtnQkFDakIsQ0FBQyxDQUFDLFNBQVMsR0FBRyxJQUFJLENBQUM7Z0JBQ25CLEtBQUksQ0FBQyxlQUFlLENBQUMsSUFBSSxDQUFDLENBQUMsQ0FBQyxDQUFDO2dCQUM3QixvQkFBb0I7Z0JBQ3BCLFlBQVksQ0FBQyxDQUFDLENBQUMsQ0FBQzthQUNqQjtpQkFBTTtnQkFDTCxDQUFDLENBQUMsU0FBUyxHQUFHLEtBQUssQ0FBQzthQUNyQjtZQUNELENBQUMsQ0FBQyxRQUFRLENBQUMsT0FBTzs7OztZQUFDLFVBQUEsS0FBSztnQkFDdEIsV0FBVyxDQUFDLEtBQUssQ0FBQyxDQUFDO1lBQ3JCLENBQUMsRUFBQyxDQUFDO1FBQ0wsQ0FBQyxDQUFBO1FBQ0QsSUFBSSxDQUFDLFNBQVMsQ0FBQyxPQUFPOzs7O1FBQUMsVUFBQSxLQUFLO1lBQzFCLFdBQVcsQ0FBQyxLQUFLLENBQUMsQ0FBQztRQUNyQixDQUFDLEVBQUMsQ0FBQztRQUNILHNCQUFzQjtRQUN0QixJQUFJLENBQUMsZ0JBQWdCLENBQUMsWUFBWSxFQUFFLElBQUksQ0FBQyxTQUFTLENBQUMsQ0FBQztJQUN0RCxDQUFDO0lBRUQ7O09BRUc7Ozs7OztJQUNILHVDQUFXOzs7OztJQUFYLFVBQVksS0FBbUI7UUFBL0IsaUJBbUJDOzs7WUFqQk8sUUFBUTs7OztRQUFHLFVBQUMsSUFBZ0I7WUFDaEMsdUJBQXVCO1lBQ3ZCLEtBQUksQ0FBQyxnQkFBZ0IsR0FBRyxLQUFJLENBQUMsZ0JBQWdCLENBQUMsTUFBTTs7OztZQUFDLFVBQUEsQ0FBQyxJQUFJLE9BQUEsQ0FBQyxDQUFDLEdBQUcsS0FBSyxJQUFJLENBQUMsR0FBRyxFQUFsQixDQUFrQixFQUFDLENBQUM7WUFDOUUsdUJBQXVCO1lBQ3ZCLEtBQUksQ0FBQyxnQkFBZ0IsR0FBRyxLQUFJLENBQUMsZ0JBQWdCLENBQUMsTUFBTTs7OztZQUFDLFVBQUEsQ0FBQyxJQUFJLE9BQUEsQ0FBQyxDQUFDLEdBQUcsS0FBSyxJQUFJLENBQUMsR0FBRyxFQUFsQixDQUFrQixFQUFDLENBQUM7WUFDOUUsc0JBQXNCO1lBQ3RCLEtBQUksQ0FBQyxlQUFlLEdBQUcsS0FBSSxDQUFDLGVBQWUsQ0FBQyxNQUFNOzs7O1lBQUMsVUFBQSxDQUFDLElBQUksT0FBQSxDQUFDLENBQUMsR0FBRyxLQUFLLElBQUksQ0FBQyxHQUFHLEVBQWxCLENBQWtCLEVBQUMsQ0FBQztZQUM1RSxJQUFJLElBQUksQ0FBQyxRQUFRLEVBQUU7Z0JBQ2pCLElBQUksQ0FBQyxRQUFRLENBQUMsT0FBTzs7OztnQkFBQyxVQUFBLEtBQUs7b0JBQ3pCLFFBQVEsQ0FBQyxLQUFLLENBQUMsQ0FBQztnQkFDbEIsQ0FBQyxFQUFDLENBQUM7YUFDSjtRQUNILENBQUMsQ0FBQTtRQUNELEtBQUssQ0FBQyxPQUFPOzs7O1FBQUMsVUFBQSxDQUFDO1lBQ2IsUUFBUSxDQUFDLENBQUMsQ0FBQyxDQUFDO1FBQ2QsQ0FBQyxFQUFDLENBQUM7UUFDSCxJQUFJLENBQUMsaUJBQWlCLENBQUMsSUFBSSxDQUFDLGVBQWUsQ0FBQyxDQUFDO0lBQy9DLENBQUM7SUFFRDs7T0FFRzs7Ozs7O0lBQ0gsMkNBQWU7Ozs7O0lBQWYsVUFBZ0IsSUFBZ0I7UUFBaEMsaUJBU0M7UUFSQyxJQUFJLElBQUksQ0FBQyxRQUFRLENBQUMsTUFBTSxLQUFLLENBQUMsRUFBRTtZQUM5QixhQUFhO1lBQ2IsSUFBSSxDQUFDLFNBQVMsQ0FBQyxJQUFJLENBQUMsQ0FBQztTQUN0QjthQUFNO1lBQ0wsSUFBSSxDQUFDLFFBQVEsQ0FBQyxPQUFPOzs7O1lBQUMsVUFBQyxLQUFLO2dCQUMxQixLQUFJLENBQUMsZUFBZSxDQUFDLEtBQUssQ0FBQyxDQUFDO1lBQzlCLENBQUMsRUFBQyxDQUFDO1NBQ0o7SUFDSCxDQUFDO0lBRUQsbUJBQW1COzs7Ozs7SUFDbkIsMENBQWM7Ozs7OztJQUFkLFVBQWUsSUFBZ0I7OztZQUN2QixVQUFVLEdBQUcsSUFBSSxDQUFDLGFBQWEsRUFBRTtRQUN2QyxJQUFJLFVBQVUsRUFBRTtZQUNkLElBQUksQ0FBQyxLQUFLLEdBQUcsVUFBVSxDQUFDLEtBQUssR0FBRyxDQUFDLENBQUM7U0FDbkM7YUFBTTtZQUNMLElBQUksQ0FBQyxLQUFLLEdBQUcsQ0FBQyxDQUFDO1NBQ2hCOztZQUNELEtBQW9CLElBQUEsS0FBQSxpQkFBQSxJQUFJLENBQUMsUUFBUSxDQUFBLGdCQUFBLDRCQUFFO2dCQUE5QixJQUFNLEtBQUssV0FBQTtnQkFDZCxJQUFJLENBQUMsY0FBYyxDQUFDLEtBQUssQ0FBQyxDQUFDO2FBQzVCOzs7Ozs7Ozs7SUFDSCxDQUFDOzs7OztJQUVELDRDQUFnQjs7OztJQUFoQixVQUFpQixLQUFnQjtRQUN2QixJQUFBLHVCQUFPOztRQUVULElBQUEsOEhBQXlJLEVBQXZJLFlBQUcsRUFBRSxrQkFBTSxFQUFFLGtCQUEwSDs7WUFDekksR0FBRyxHQUFHLElBQUksQ0FBQyxHQUFHLENBQUMsTUFBTSxHQUFHLElBQUksQ0FBQyxlQUFlLEVBQUUsSUFBSSxDQUFDLFlBQVksQ0FBQztRQUV0RSxJQUFJLE9BQU8sSUFBSSxHQUFHLEdBQUcsR0FBRyxFQUFFO1lBQ3hCLE9BQU8sQ0FBQyxDQUFDLENBQUM7U0FDWDthQUFNLElBQUksT0FBTyxJQUFJLE1BQU0sR0FBRyxHQUFHLEVBQUU7WUFDbEMsT0FBTyxDQUFDLENBQUM7U0FDVjtRQUVELE9BQU8sQ0FBQyxDQUFDO0lBQ1gsQ0FBQztJQUVEOzs7T0FHRzs7Ozs7Ozs7SUFDSCx3Q0FBWTs7Ozs7OztJQUFaLFVBQWEsVUFBc0IsRUFBRSxPQUFvQjtRQUF6RCxpQkEyQ0M7UUEzQ29DLHdCQUFBLEVBQUEsV0FBbUIsQ0FBQztRQUN2RCxJQUFJLENBQUMsVUFBVSxJQUFJLE9BQU8sR0FBRyxDQUFDLEVBQUU7WUFDOUIsT0FBTztTQUNSOztZQUNLLFdBQVcsR0FBRyxVQUFVLENBQUMsV0FBVzs7WUFDcEMsWUFBWSxHQUFHLFVBQVUsQ0FBQyxhQUFhLEVBQUU7O1lBQ3pDLGtCQUFrQixHQUFHLElBQUksQ0FBQyxZQUFZLENBQUMsYUFBYSxFQUFFO1FBQzVELHNCQUFzQjtRQUN0QixJQUFJLGtCQUFrQixFQUFFO1lBQ3RCLGtCQUFrQixDQUFDLFFBQVEsR0FBRyxrQkFBa0IsQ0FBQyxRQUFRLENBQUMsTUFBTTs7OztZQUFDLFVBQUEsQ0FBQyxJQUFJLE9BQUEsQ0FBQyxDQUFDLEdBQUcsS0FBSyxLQUFJLENBQUMsWUFBWSxDQUFDLEdBQUcsRUFBL0IsQ0FBK0IsRUFBQyxDQUFDO1NBQ3hHO2FBQU07WUFDTCxJQUFJLENBQUMsU0FBUyxHQUFHLElBQUksQ0FBQyxTQUFTLENBQUMsTUFBTTs7OztZQUFDLFVBQUEsQ0FBQyxJQUFJLE9BQUEsQ0FBQyxDQUFDLEdBQUcsS0FBSyxLQUFJLENBQUMsWUFBWSxDQUFDLEdBQUcsRUFBL0IsQ0FBK0IsRUFBQyxDQUFDO1NBQzlFO1FBQ0QsUUFBUSxPQUFPLEVBQUU7WUFDZixLQUFLLENBQUM7Z0JBQ0osVUFBVSxDQUFDLFdBQVcsQ0FBQyxDQUFFLElBQUksQ0FBQyxZQUFZLENBQUUsQ0FBQyxDQUFDO2dCQUM5QyxJQUFJLENBQUMsY0FBYyxDQUFDLFVBQVUsQ0FBQyxDQUFDO2dCQUNoQyxNQUFNO1lBQ1IsS0FBSyxDQUFDLENBQUMsQ0FBQztZQUNSLEtBQUssQ0FBQzs7b0JBQ0UsTUFBTSxHQUFHLE9BQU8sS0FBSyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQztnQkFDcEMsSUFBSSxZQUFZLEVBQUU7b0JBQ2hCLFlBQVksQ0FBQyxXQUFXLENBQUMsQ0FBRSxJQUFJLENBQUMsWUFBWSxDQUFFLEVBQUUsWUFBWSxDQUFDLFFBQVEsQ0FBQyxPQUFPLENBQUMsVUFBVSxDQUFDLEdBQUcsTUFBTSxDQUFDLENBQUM7O3dCQUM5RixVQUFVLEdBQUcsSUFBSSxDQUFDLFlBQVksQ0FBQyxhQUFhLEVBQUU7b0JBQ3BELElBQUksVUFBVSxFQUFFO3dCQUNkLElBQUksQ0FBQyxjQUFjLENBQUMsVUFBVSxDQUFDLENBQUM7cUJBQ2pDO2lCQUNGO3FCQUFNOzt3QkFDQyxXQUFXLEdBQUcsSUFBSSxDQUFDLFNBQVMsQ0FBQyxPQUFPLENBQUMsVUFBVSxDQUFDLEdBQUcsTUFBTTtvQkFDL0QsUUFBUTtvQkFDUixJQUFJLENBQUMsU0FBUyxDQUFDLE1BQU0sQ0FBQyxXQUFXLEVBQUUsQ0FBQyxFQUFFLElBQUksQ0FBQyxZQUFZLENBQUMsQ0FBQztvQkFDekQsSUFBSSxDQUFDLFNBQVMsQ0FBRSxXQUFXLENBQUUsQ0FBQyxVQUFVLEdBQUcsSUFBSSxDQUFDO29CQUNoRCxJQUFJLENBQUMsU0FBUyxDQUFFLFdBQVcsQ0FBRSxDQUFDLEtBQUssR0FBRyxDQUFDLENBQUM7aUJBQ3pDO2dCQUNELE1BQU07U0FDVDtRQUNELGtCQUFrQjtRQUNsQixJQUFJLENBQUMsU0FBUyxDQUFDLE9BQU87Ozs7UUFBQyxVQUFDLEtBQUs7WUFDM0IsSUFBSSxDQUFDLEtBQUssQ0FBQyxXQUFXLEVBQUU7Z0JBQ3RCLEtBQUssQ0FBQyxPQUFPLEdBQUcsV0FBVyxDQUFDO2FBQzdCO1lBQ0QsS0FBSSxDQUFDLGVBQWUsQ0FBQyxLQUFLLENBQUMsQ0FBQztRQUM5QixDQUFDLEVBQUMsQ0FBQztJQUNMLENBQUM7SUFFRDs7Ozs7O09BTUc7Ozs7Ozs7Ozs7OztJQUNILHVDQUFXOzs7Ozs7Ozs7OztJQUFYLFVBQVksU0FBaUIsRUFBRSxJQUF1QixFQUFFLEtBQW9DOztZQUNwRixhQUFhLEdBQXNCO1lBQ3ZDLFdBQVcsRUFBRSxTQUFTO1lBQ3RCLE1BQU0sRUFBTyxJQUFJO1lBQ2pCLE9BQU8sRUFBTSxLQUFLO1NBQ25CO1FBQ0QsUUFBUSxTQUFTLEVBQUU7WUFDakIsS0FBSyxXQUFXLENBQUM7WUFDakIsS0FBSyxXQUFXLENBQUM7WUFDakIsS0FBSyxVQUFVLENBQUM7WUFDaEIsS0FBSyxXQUFXLENBQUM7WUFDakIsS0FBSyxNQUFNLENBQUM7WUFDWixLQUFLLFNBQVM7Z0JBQ1osTUFBTSxDQUFDLE1BQU0sQ0FBQyxhQUFhLEVBQUUsRUFBRSxVQUFVLEVBQUUsSUFBSSxDQUFDLGVBQWUsRUFBRSxFQUFFLENBQUMsQ0FBQztnQkFDckUsTUFBTTtZQUNSLEtBQUssT0FBTyxDQUFDO1lBQ2IsS0FBSyxVQUFVO2dCQUNiLE1BQU0sQ0FBQyxNQUFNLENBQUMsYUFBYSxFQUFFLEVBQUUsY0FBYyxFQUFFLElBQUksQ0FBQyxnQkFBZ0IsRUFBRSxDQUFDLENBQUM7Z0JBQ3hFLE1BQU0sQ0FBQyxNQUFNLENBQUMsYUFBYSxFQUFFLEVBQUUsT0FBTyxFQUFFLElBQUksQ0FBQyxnQkFBZ0IsRUFBRSxDQUFDLENBQUM7Z0JBQ2pFLE1BQU0sQ0FBQyxNQUFNLENBQUMsYUFBYSxFQUFFLEVBQUUsTUFBTSxFQUFFLElBQUksQ0FBQyxnQkFBZ0IsQ0FBQyxHQUFHOzs7O29CQUFDLFVBQUEsQ0FBQyxJQUFJLE9BQUEsQ0FBQyxDQUFDLEdBQUcsRUFBTCxDQUFLLEVBQUMsRUFBRSxDQUFDLENBQUM7Z0JBQ2hGLE1BQU07WUFDUixLQUFLLE9BQU87O29CQUNKLGVBQWUsR0FBRyxJQUFJLENBQUMsa0JBQWtCLEVBQUU7Z0JBRWpELE1BQU0sQ0FBQyxNQUFNLENBQUMsYUFBYSxFQUFFLEVBQUUsYUFBYSxFQUFFLGVBQWUsRUFBRSxDQUFDLENBQUM7Z0JBQ2pFLE1BQU0sQ0FBQyxNQUFNLENBQUMsYUFBYSxFQUFFLEVBQUUsT0FBTyxFQUFFLGVBQWUsRUFBRSxDQUFDLENBQUM7Z0JBQzNELE1BQU0sQ0FBQyxNQUFNLENBQUMsYUFBYSxFQUFFLEVBQUUsTUFBTSxFQUFFLGVBQWUsQ0FBQyxHQUFHOzs7O29CQUFDLFVBQUEsQ0FBQyxJQUFJLE9BQUEsQ0FBQyxDQUFDLEdBQUcsRUFBTCxDQUFLLEVBQUMsRUFBRSxDQUFDLENBQUM7Z0JBQzFFLE1BQU07WUFDUixLQUFLLFFBQVE7Z0JBQ1gsTUFBTSxDQUFDLE1BQU0sQ0FBQyxhQUFhLEVBQUUsRUFBRSxhQUFhLEVBQUUsSUFBSSxDQUFDLGtCQUFrQixFQUFFLEVBQUUsQ0FBQyxDQUFDO2dCQUMzRSxNQUFNLENBQUMsTUFBTSxDQUFDLGFBQWEsRUFBRSxFQUFFLE9BQU8sRUFBRSxJQUFJLENBQUMsa0JBQWtCLEVBQUUsRUFBRSxDQUFDLENBQUM7Z0JBQ3JFLE1BQU0sQ0FBQyxNQUFNLENBQUMsYUFBYSxFQUFFLEVBQUUsTUFBTSxFQUFFLElBQUksQ0FBQyxrQkFBa0IsRUFBRSxDQUFDLEdBQUc7Ozs7b0JBQUMsVUFBQSxDQUFDLElBQUksT0FBQSxDQUFDLENBQUMsR0FBRyxFQUFMLENBQUssRUFBQyxFQUFFLENBQUMsQ0FBQztnQkFDcEYsTUFBTTtZQUNSLEtBQUssUUFBUTtnQkFDWCxNQUFNLENBQUMsTUFBTSxDQUFDLGFBQWEsRUFBRSxFQUFFLE9BQU8sRUFBRSxJQUFJLENBQUMsZ0JBQWdCLEVBQUUsQ0FBQyxDQUFDO2dCQUNqRSxNQUFNLENBQUMsTUFBTSxDQUFDLGFBQWEsRUFBRSxFQUFFLE1BQU0sRUFBRSxJQUFJLENBQUMsZ0JBQWdCLENBQUMsR0FBRzs7OztvQkFBQyxVQUFBLENBQUMsSUFBSSxPQUFBLENBQUMsQ0FBQyxHQUFHLEVBQUwsQ0FBSyxFQUFDLEVBQUUsQ0FBQyxDQUFDO2dCQUNoRixNQUFNO1NBQ1Q7UUFDRCxPQUFPLGFBQWEsQ0FBQztJQUN2QixDQUFDOzs7O0lBRUQsdUNBQVc7OztJQUFYO1FBQ0UsSUFBSSxDQUFDLG1CQUFtQixDQUFDLFFBQVEsRUFBRSxDQUFDO0lBQ3RDLENBQUM7O2dCQTVoQkYsVUFBVTs7SUE2aEJYLHdCQUFDO0NBQUEsQUE3aEJELElBNmhCQztTQTVoQlksaUJBQWlCOzs7SUFDNUIsNENBQXVCOztJQUN2Qix5Q0FBaUI7O0lBRWpCLDRDQUFpQzs7SUFDakMsdUNBQTRCOztJQUM1Qix5Q0FBeUI7O0lBQ3pCLHNDQUE2Qjs7SUFDN0IsNkNBQW9DOztJQUNwQyw2Q0FBb0M7O0lBQ3BDLDRDQUFtQzs7SUFDbkMsZ0RBQXVDOztJQUN2Qyw0Q0FBbUM7O0lBQ25DLGdEQUF1RCIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7IEluamVjdGFibGUsIE9uRGVzdHJveSB9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuaW1wb3J0IHsgT2JzZXJ2YWJsZSwgU3ViamVjdCB9IGZyb20gJ3J4anMnO1xuaW1wb3J0IHsgaXNOb3ROaWwgfSBmcm9tICcuLi9jb3JlL3V0aWwvY2hlY2snO1xuaW1wb3J0IHsgTnpGb3JtYXRFbWl0RXZlbnQgfSBmcm9tICcuL2ludGVyZmFjZSc7XG5pbXBvcnQgeyBOelRyZWVOb2RlIH0gZnJvbSAnLi9uei10cmVlLW5vZGUnO1xuaW1wb3J0IHsgaXNDaGVja0Rpc2FibGVkLCBpc0luQXJyYXkgfSBmcm9tICcuL256LXRyZWUtdXRpbCc7XG5cbkBJbmplY3RhYmxlKClcbmV4cG9ydCBjbGFzcyBOelRyZWVCYXNlU2VydmljZSBpbXBsZW1lbnRzIE9uRGVzdHJveSB7XG4gIERSQUdfU0lERV9SQU5HRSA9IDAuMjU7XG4gIERSQUdfTUlOX0dBUCA9IDI7XG5cbiAgaXNDaGVja1N0cmljdGx5OiBib29sZWFuID0gZmFsc2U7XG4gIGlzTXVsdGlwbGU6IGJvb2xlYW4gPSBmYWxzZTtcbiAgc2VsZWN0ZWROb2RlOiBOelRyZWVOb2RlO1xuICByb290Tm9kZXM6IE56VHJlZU5vZGVbXSA9IFtdO1xuICBzZWxlY3RlZE5vZGVMaXN0OiBOelRyZWVOb2RlW10gPSBbXTtcbiAgZXhwYW5kZWROb2RlTGlzdDogTnpUcmVlTm9kZVtdID0gW107XG4gIGNoZWNrZWROb2RlTGlzdDogTnpUcmVlTm9kZVtdID0gW107XG4gIGhhbGZDaGVja2VkTm9kZUxpc3Q6IE56VHJlZU5vZGVbXSA9IFtdO1xuICBtYXRjaGVkTm9kZUxpc3Q6IE56VHJlZU5vZGVbXSA9IFtdO1xuICB0cmlnZ2VyRXZlbnRDaGFuZ2UkID0gbmV3IFN1YmplY3Q8TnpGb3JtYXRFbWl0RXZlbnQ+KCk7XG5cbiAgLyoqXG4gICAqIHRyaWdnZXIgZXZlbnRcbiAgICovXG4gIGV2ZW50VHJpZ2dlckNoYW5nZWQoKTogT2JzZXJ2YWJsZTxOekZvcm1hdEVtaXRFdmVudD4ge1xuICAgIHJldHVybiB0aGlzLnRyaWdnZXJFdmVudENoYW5nZSQuYXNPYnNlcnZhYmxlKCk7XG4gIH1cblxuICAvKipcbiAgICogcmVzZXQgdHJlZSBub2RlcyB3aWxsIGNsZWFyIGRlZmF1bHQgbm9kZSBsaXN0XG4gICAqL1xuICBpbml0VHJlZShuek5vZGVzOiBOelRyZWVOb2RlW10pOiB2b2lkIHtcbiAgICB0aGlzLnJvb3ROb2RlcyA9IG56Tm9kZXM7XG4gICAgdGhpcy5leHBhbmRlZE5vZGVMaXN0ID0gW107XG4gICAgdGhpcy5zZWxlY3RlZE5vZGVMaXN0ID0gW107XG4gICAgdGhpcy5oYWxmQ2hlY2tlZE5vZGVMaXN0ID0gW107XG4gICAgdGhpcy5jaGVja2VkTm9kZUxpc3QgPSBbXTtcbiAgICB0aGlzLm1hdGNoZWROb2RlTGlzdCA9IFtdO1xuICAgIC8vIHJlZnJlc2ggbm9kZSBjaGVja2VkIHN0YXRlXG4gICAgc2V0VGltZW91dCgoKSA9PiB7XG4gICAgICB0aGlzLnJlZnJlc2hDaGVja1N0YXRlKHRoaXMuaXNDaGVja1N0cmljdGx5KTtcbiAgICB9KTtcbiAgfVxuXG4gIGdldFNlbGVjdGVkTm9kZSgpOiBOelRyZWVOb2RlIHwgbnVsbCB7XG4gICAgcmV0dXJuIHRoaXMuc2VsZWN0ZWROb2RlO1xuICB9XG5cbiAgLyoqXG4gICAqIGdldCBzb21lIGxpc3RcbiAgICovXG4gIGdldFNlbGVjdGVkTm9kZUxpc3QoKTogTnpUcmVlTm9kZVtdIHtcbiAgICByZXR1cm4gdGhpcy5jb25kdWN0Tm9kZVN0YXRlKCdzZWxlY3QnKTtcbiAgfVxuXG4gIC8qKlxuICAgKiByZXR1cm4gY2hlY2tlZCBub2Rlc1xuICAgKi9cbiAgZ2V0Q2hlY2tlZE5vZGVMaXN0KCk6IE56VHJlZU5vZGVbXSB7XG4gICAgcmV0dXJuIHRoaXMuY29uZHVjdE5vZGVTdGF0ZSgnY2hlY2snKTtcbiAgfVxuXG4gIGdldEhhbGZDaGVja2VkTm9kZUxpc3QoKTogTnpUcmVlTm9kZVtdIHtcbiAgICByZXR1cm4gdGhpcy5jb25kdWN0Tm9kZVN0YXRlKCdoYWxmQ2hlY2snKTtcbiAgfVxuXG4gIC8qKlxuICAgKiByZXR1cm4gZXhwYW5kZWQgbm9kZXNcbiAgICovXG4gIGdldEV4cGFuZGVkTm9kZUxpc3QoKTogTnpUcmVlTm9kZVtdIHtcbiAgICByZXR1cm4gdGhpcy5jb25kdWN0Tm9kZVN0YXRlKCdleHBhbmQnKTtcbiAgfVxuXG4gIC8qKlxuICAgKiByZXR1cm4gc2VhcmNoIG1hdGNoZWQgbm9kZXNcbiAgICovXG4gIGdldE1hdGNoZWROb2RlTGlzdCgpOiBOelRyZWVOb2RlW10ge1xuICAgIHJldHVybiB0aGlzLmNvbmR1Y3ROb2RlU3RhdGUoJ21hdGNoJyk7XG4gIH1cblxuICAvLyB0c2xpbnQ6ZGlzYWJsZS1uZXh0LWxpbmU6bm8tYW55XG4gIGlzQXJyYXlPZk56VHJlZU5vZGUodmFsdWU6IGFueVtdKTogYm9vbGVhbiB7XG4gICAgcmV0dXJuIHZhbHVlLmV2ZXJ5KGl0ZW0gPT4gaXRlbSBpbnN0YW5jZW9mIE56VHJlZU5vZGUpO1xuICB9XG5cbiAgLyoqXG4gICAqIHJlc2V0IHNlbGVjdGVkTm9kZUxpc3RcbiAgICovXG4gIGNhbGNTZWxlY3RlZEtleXMoc2VsZWN0ZWRLZXlzOiBzdHJpbmdbXSwgbnpOb2RlczogTnpUcmVlTm9kZVtdLCBpc011bHRpOiBib29sZWFuID0gZmFsc2UpOiB2b2lkIHtcbiAgICBjb25zdCBjYWxjID0gKG5vZGVzOiBOelRyZWVOb2RlW10pOiBib29sZWFuID0+IHtcbiAgICAgIHJldHVybiBub2Rlcy5ldmVyeShub2RlID0+IHtcbiAgICAgICAgaWYgKGlzSW5BcnJheShub2RlLmtleSwgc2VsZWN0ZWRLZXlzKSkge1xuICAgICAgICAgIG5vZGUuaXNTZWxlY3RlZCA9IHRydWU7XG4gICAgICAgICAgaWYgKCFpc011bHRpKSB7XG4gICAgICAgICAgICAvLyBpZiBub3Qgc3VwcG9ydCBtdWx0aSBzZWxlY3RcbiAgICAgICAgICAgIHJldHVybiBmYWxzZTtcbiAgICAgICAgICB9XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgbm9kZS5pc1NlbGVjdGVkID0gZmFsc2U7XG4gICAgICAgIH1cbiAgICAgICAgaWYgKG5vZGUuY2hpbGRyZW4ubGVuZ3RoID4gMCkge1xuICAgICAgICAgIC8vIFJlY3Vyc2lvblxuICAgICAgICAgIHJldHVybiBjYWxjKG5vZGUuY2hpbGRyZW4pO1xuICAgICAgICB9XG4gICAgICAgIHJldHVybiB0cnVlO1xuICAgICAgfSk7XG4gICAgfTtcbiAgICBjYWxjKG56Tm9kZXMpO1xuICB9XG5cbiAgLyoqXG4gICAqIHJlc2V0IGV4cGFuZGVkTm9kZUxpc3RcbiAgICovXG4gIGNhbGNFeHBhbmRlZEtleXMoZXhwYW5kZWRLZXlzOiBzdHJpbmdbXSwgbnpOb2RlczogTnpUcmVlTm9kZVtdKTogdm9pZCB7XG4gICAgdGhpcy5leHBhbmRlZE5vZGVMaXN0ID0gW107XG4gICAgY29uc3QgY2FsYyA9IChub2RlczogTnpUcmVlTm9kZVtdKSA9PiB7XG4gICAgICBub2Rlcy5mb3JFYWNoKG5vZGUgPT4ge1xuICAgICAgICBpZiAoaXNJbkFycmF5KG5vZGUua2V5LCBleHBhbmRlZEtleXMpKSB7XG4gICAgICAgICAgbm9kZS5pc0V4cGFuZGVkID0gdHJ1ZTtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICBub2RlLmlzRXhwYW5kZWQgPSBmYWxzZTtcbiAgICAgICAgfVxuICAgICAgICBpZiAobm9kZS5jaGlsZHJlbi5sZW5ndGggPiAwKSB7XG4gICAgICAgICAgY2FsYyhub2RlLmNoaWxkcmVuKTtcbiAgICAgICAgfVxuICAgICAgfSk7XG4gICAgfTtcbiAgICBjYWxjKG56Tm9kZXMpO1xuICB9XG5cbiAgLyoqXG4gICAqIHJlc2V0IGNoZWNrZWROb2RlTGlzdFxuICAgKi9cbiAgY2FsY0NoZWNrZWRLZXlzKGNoZWNrZWRLZXlzOiBzdHJpbmdbXSwgbnpOb2RlczogTnpUcmVlTm9kZVtdLCBpc0NoZWNrU3RyaWN0bHk6IGJvb2xlYW4gPSBmYWxzZSk6IHZvaWQge1xuICAgIHRoaXMuY2hlY2tlZE5vZGVMaXN0ID0gW107XG4gICAgdGhpcy5oYWxmQ2hlY2tlZE5vZGVMaXN0ID0gW107XG4gICAgY29uc3QgY2FsYyA9IChub2RlczogTnpUcmVlTm9kZVtdKSA9PiB7XG4gICAgICBub2Rlcy5mb3JFYWNoKG5vZGUgPT4ge1xuICAgICAgICBpZiAoaXNJbkFycmF5KG5vZGUua2V5LCBjaGVja2VkS2V5cykpIHtcbiAgICAgICAgICBub2RlLmlzQ2hlY2tlZCA9IHRydWU7XG4gICAgICAgICAgbm9kZS5pc0hhbGZDaGVja2VkID0gZmFsc2U7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgbm9kZS5pc0NoZWNrZWQgPSBmYWxzZTtcbiAgICAgICAgICBub2RlLmlzSGFsZkNoZWNrZWQgPSBmYWxzZTtcbiAgICAgICAgfVxuICAgICAgICBpZiAobm9kZS5jaGlsZHJlbi5sZW5ndGggPiAwKSB7XG4gICAgICAgICAgY2FsYyhub2RlLmNoaWxkcmVuKTtcbiAgICAgICAgfVxuICAgICAgfSk7XG4gICAgfTtcbiAgICBjYWxjKG56Tm9kZXMpO1xuICAgIC8vIGNvbnRyb2xsZWQgc3RhdGVcbiAgICB0aGlzLnJlZnJlc2hDaGVja1N0YXRlKGlzQ2hlY2tTdHJpY3RseSk7XG4gIH1cblxuICAvKipcbiAgICogc2V0IGRyYWcgbm9kZVxuICAgKi9cbiAgc2V0U2VsZWN0ZWROb2RlKG5vZGU6IE56VHJlZU5vZGUpOiB2b2lkIHtcbiAgICB0aGlzLnNlbGVjdGVkTm9kZSA9IG5vZGU7XG4gIH1cblxuICAvKipcbiAgICogc2V0IG5vZGUgc2VsZWN0ZWQgc3RhdHVzXG4gICAqL1xuICBzZXROb2RlQWN0aXZlKG5vZGU6IE56VHJlZU5vZGUpOiB2b2lkIHtcbiAgICBpZiAoIXRoaXMuaXNNdWx0aXBsZSAmJiBub2RlLmlzU2VsZWN0ZWQpIHtcbiAgICAgIHRoaXMuc2VsZWN0ZWROb2RlTGlzdC5mb3JFYWNoKG4gPT4ge1xuICAgICAgICBpZiAobm9kZS5rZXkgIT09IG4ua2V5KSB7XG4gICAgICAgICAgLy8gcmVzZXQgb3RoZXIgbm9kZXNcbiAgICAgICAgICBuLmlzU2VsZWN0ZWQgPSBmYWxzZTtcbiAgICAgICAgfVxuICAgICAgfSk7XG4gICAgICAvLyBzaW5nbGUgbW9kZTogcmVtb3ZlIHByZSBub2RlXG4gICAgICB0aGlzLnNlbGVjdGVkTm9kZUxpc3QgPSBbXTtcbiAgICB9XG4gICAgdGhpcy5zZXRTZWxlY3RlZE5vZGVMaXN0KG5vZGUsIHRoaXMuaXNNdWx0aXBsZSk7XG4gIH1cblxuICAvKipcbiAgICogYWRkIG9yIHJlbW92ZSBub2RlIHRvIHNlbGVjdGVkTm9kZUxpc3RcbiAgICovXG4gIHNldFNlbGVjdGVkTm9kZUxpc3Qobm9kZTogTnpUcmVlTm9kZSwgaXNNdWx0aXBsZTogYm9vbGVhbiA9IGZhbHNlKTogdm9pZCB7XG4gICAgY29uc3QgaW5kZXggPSB0aGlzLnNlbGVjdGVkTm9kZUxpc3QuZmluZEluZGV4KG4gPT4gbm9kZS5rZXkgPT09IG4ua2V5KTtcbiAgICBpZiAoaXNNdWx0aXBsZSkge1xuICAgICAgaWYgKG5vZGUuaXNTZWxlY3RlZCAmJiBpbmRleCA9PT0gLTEpIHtcbiAgICAgICAgdGhpcy5zZWxlY3RlZE5vZGVMaXN0LnB1c2gobm9kZSk7XG4gICAgICB9XG4gICAgfSBlbHNlIHtcbiAgICAgIGlmIChub2RlLmlzU2VsZWN0ZWQgJiYgaW5kZXggPT09IC0xKSB7XG4gICAgICAgIHRoaXMuc2VsZWN0ZWROb2RlTGlzdCA9IFsgbm9kZSBdO1xuICAgICAgfVxuICAgIH1cbiAgICBpZiAoIW5vZGUuaXNTZWxlY3RlZCkge1xuICAgICAgdGhpcy5zZWxlY3RlZE5vZGVMaXN0ID0gdGhpcy5zZWxlY3RlZE5vZGVMaXN0LmZpbHRlcihuID0+IChuLmtleSAhPT0gbm9kZS5rZXkpKTtcbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogbWVyZ2UgY2hlY2tlZCBub2Rlc1xuICAgKi9cbiAgc2V0SGFsZkNoZWNrZWROb2RlTGlzdChub2RlOiBOelRyZWVOb2RlKTogdm9pZCB7XG4gICAgY29uc3QgaW5kZXggPSB0aGlzLmhhbGZDaGVja2VkTm9kZUxpc3QuZmluZEluZGV4KG4gPT4gbm9kZS5rZXkgPT09IG4ua2V5KTtcbiAgICBpZiAobm9kZS5pc0hhbGZDaGVja2VkICYmIGluZGV4ID09PSAtMSkge1xuICAgICAgdGhpcy5oYWxmQ2hlY2tlZE5vZGVMaXN0LnB1c2gobm9kZSk7XG4gICAgfSBlbHNlIGlmICghbm9kZS5pc0hhbGZDaGVja2VkICYmIGluZGV4ID4gLTEpIHtcbiAgICAgIHRoaXMuaGFsZkNoZWNrZWROb2RlTGlzdCA9IHRoaXMuaGFsZkNoZWNrZWROb2RlTGlzdC5maWx0ZXIobiA9PiBub2RlLmtleSAhPT0gbi5rZXkpO1xuICAgIH1cbiAgfVxuXG4gIHNldENoZWNrZWROb2RlTGlzdChub2RlOiBOelRyZWVOb2RlKTogdm9pZCB7XG4gICAgY29uc3QgaW5kZXggPSB0aGlzLmNoZWNrZWROb2RlTGlzdC5maW5kSW5kZXgobiA9PiBub2RlLmtleSA9PT0gbi5rZXkpO1xuICAgIGlmIChub2RlLmlzQ2hlY2tlZCAmJiBpbmRleCA9PT0gLTEpIHtcbiAgICAgIHRoaXMuY2hlY2tlZE5vZGVMaXN0LnB1c2gobm9kZSk7XG4gICAgfSBlbHNlIGlmICghbm9kZS5pc0NoZWNrZWQgJiYgaW5kZXggPiAtMSkge1xuICAgICAgdGhpcy5jaGVja2VkTm9kZUxpc3QgPSB0aGlzLmNoZWNrZWROb2RlTGlzdC5maWx0ZXIobiA9PiBub2RlLmtleSAhPT0gbi5rZXkpO1xuICAgIH1cbiAgfVxuXG4gIC8qKlxuICAgKiBjb25kdWN0IGNoZWNrZWQvc2VsZWN0ZWQvZXhwYW5kZWQga2V5c1xuICAgKi9cbiAgY29uZHVjdE5vZGVTdGF0ZSh0eXBlOiBzdHJpbmcgPSAnY2hlY2snKTogTnpUcmVlTm9kZVtdIHtcbiAgICBsZXQgcmVzdWx0Tm9kZXNMaXN0OiBOelRyZWVOb2RlW10gPSBbXTtcbiAgICBzd2l0Y2ggKHR5cGUpIHtcbiAgICAgIGNhc2UgJ3NlbGVjdCc6XG4gICAgICAgIHJlc3VsdE5vZGVzTGlzdCA9IHRoaXMuc2VsZWN0ZWROb2RlTGlzdDtcbiAgICAgICAgYnJlYWs7XG4gICAgICBjYXNlICdleHBhbmQnOlxuICAgICAgICByZXN1bHROb2Rlc0xpc3QgPSB0aGlzLmV4cGFuZGVkTm9kZUxpc3Q7XG4gICAgICAgIGJyZWFrO1xuICAgICAgY2FzZSAnbWF0Y2gnOlxuICAgICAgICByZXN1bHROb2Rlc0xpc3QgPSB0aGlzLm1hdGNoZWROb2RlTGlzdDtcbiAgICAgICAgYnJlYWs7XG4gICAgICBjYXNlICdjaGVjayc6XG4gICAgICAgIHJlc3VsdE5vZGVzTGlzdCA9IHRoaXMuY2hlY2tlZE5vZGVMaXN0O1xuICAgICAgICBjb25zdCBpc0lnbm9yZSA9IChub2RlOiBOelRyZWVOb2RlKTogYm9vbGVhbiA9PiB7XG4gICAgICAgICAgY29uc3QgcGFyZW50Tm9kZSA9IG5vZGUuZ2V0UGFyZW50Tm9kZSgpO1xuICAgICAgICAgIGlmIChwYXJlbnROb2RlKSB7XG4gICAgICAgICAgICBpZiAodGhpcy5jaGVja2VkTm9kZUxpc3QuZmluZEluZGV4KG4gPT4gbi5rZXkgPT09IHBhcmVudE5vZGUua2V5KSA+IC0xKSB7XG4gICAgICAgICAgICAgIHJldHVybiB0cnVlO1xuICAgICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgICAgcmV0dXJuIGlzSWdub3JlKHBhcmVudE5vZGUpO1xuICAgICAgICAgICAgfVxuICAgICAgICAgIH1cbiAgICAgICAgICByZXR1cm4gZmFsc2U7XG4gICAgICAgIH07XG4gICAgICAgIC8vIG1lcmdlIGNoZWNrZWRcbiAgICAgICAgaWYgKCF0aGlzLmlzQ2hlY2tTdHJpY3RseSkge1xuICAgICAgICAgIHJlc3VsdE5vZGVzTGlzdCA9IHRoaXMuY2hlY2tlZE5vZGVMaXN0LmZpbHRlcihuID0+ICFpc0lnbm9yZShuKSk7XG4gICAgICAgIH1cbiAgICAgICAgYnJlYWs7XG4gICAgICBjYXNlICdoYWxmQ2hlY2snOlxuICAgICAgICBpZiAoIXRoaXMuaXNDaGVja1N0cmljdGx5KSB7XG4gICAgICAgICAgcmVzdWx0Tm9kZXNMaXN0ID0gdGhpcy5oYWxmQ2hlY2tlZE5vZGVMaXN0O1xuICAgICAgICB9XG4gICAgICAgIGJyZWFrO1xuICAgIH1cbiAgICByZXR1cm4gcmVzdWx0Tm9kZXNMaXN0O1xuICB9XG5cbiAgLyoqXG4gICAqIHNldCBleHBhbmRlZCBub2Rlc1xuICAgKi9cbiAgc2V0RXhwYW5kZWROb2RlTGlzdChub2RlOiBOelRyZWVOb2RlKTogdm9pZCB7XG4gICAgaWYgKG5vZGUuaXNMZWFmKSB7XG4gICAgICByZXR1cm47XG4gICAgfVxuICAgIGNvbnN0IGluZGV4ID0gdGhpcy5leHBhbmRlZE5vZGVMaXN0LmZpbmRJbmRleChuID0+IG5vZGUua2V5ID09PSBuLmtleSk7XG4gICAgaWYgKG5vZGUuaXNFeHBhbmRlZCAmJiBpbmRleCA9PT0gLTEpIHtcbiAgICAgIHRoaXMuZXhwYW5kZWROb2RlTGlzdC5wdXNoKG5vZGUpO1xuICAgIH0gZWxzZSBpZiAoIW5vZGUuaXNFeHBhbmRlZCAmJiBpbmRleCA+IC0xKSB7XG4gICAgICB0aGlzLmV4cGFuZGVkTm9kZUxpc3QgPSB0aGlzLmV4cGFuZGVkTm9kZUxpc3QuZmlsdGVyKG4gPT4gbm9kZS5rZXkgIT09IG4ua2V5KTtcbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogY2hlY2sgc3RhdGVcbiAgICogQHBhcmFtIG5vZGVcbiAgICovXG4gIHJlZnJlc2hDaGVja1N0YXRlKGlzQ2hlY2tTdHJpY3RseTogYm9vbGVhbiA9IGZhbHNlKTogdm9pZCB7XG4gICAgaWYgKGlzQ2hlY2tTdHJpY3RseSkge1xuICAgICAgcmV0dXJuO1xuICAgIH1cbiAgICB0aGlzLmNoZWNrZWROb2RlTGlzdC5mb3JFYWNoKG5vZGUgPT4ge1xuICAgICAgdGhpcy5jb25kdWN0KG5vZGUpO1xuICAgIH0pO1xuICB9XG5cbiAgLy8gcmVzZXQgb3RoZXIgbm9kZSBjaGVja2VkIHN0YXRlIGJhc2VkIGN1cnJlbnQgbm9kZVxuICBjb25kdWN0KG5vZGU6IE56VHJlZU5vZGUpOiB2b2lkIHtcbiAgICBjb25zdCBpc0NoZWNrZWQgPSBub2RlLmlzQ2hlY2tlZDtcbiAgICBpZiAobm9kZSkge1xuICAgICAgdGhpcy5jb25kdWN0VXAobm9kZSk7XG4gICAgICB0aGlzLmNvbmR1Y3REb3duKG5vZGUsIGlzQ2hlY2tlZCk7XG4gICAgfVxuICB9XG5cbiAgLyoqXG4gICAqIDHjgIFjaGlsZHJlbiBoYWxmIGNoZWNrZWRcbiAgICogMuOAgWNoaWxkcmVuIGFsbCBjaGVja2VkLCBwYXJlbnQgY2hlY2tlZFxuICAgKiAz44CBbm8gY2hpbGRyZW4gY2hlY2tlZFxuICAgKi9cbiAgY29uZHVjdFVwKG5vZGU6IE56VHJlZU5vZGUpOiB2b2lkIHtcbiAgICBjb25zdCBwYXJlbnROb2RlID0gbm9kZS5nZXRQYXJlbnROb2RlKCk7XG4gICAgLy8g5YWo56aB55So6IqC54K55LiN6YCJ5LitXG4gICAgaWYgKHBhcmVudE5vZGUpIHtcbiAgICAgIGlmICghaXNDaGVja0Rpc2FibGVkKHBhcmVudE5vZGUpKSB7XG4gICAgICAgIGlmIChwYXJlbnROb2RlLmNoaWxkcmVuLmV2ZXJ5KGNoaWxkID0+IGlzQ2hlY2tEaXNhYmxlZChjaGlsZCkgfHwgKCFjaGlsZC5pc0hhbGZDaGVja2VkICYmIGNoaWxkLmlzQ2hlY2tlZCkpKSB7XG4gICAgICAgICAgcGFyZW50Tm9kZS5pc0NoZWNrZWQgPSB0cnVlO1xuICAgICAgICAgIHBhcmVudE5vZGUuaXNIYWxmQ2hlY2tlZCA9IGZhbHNlO1xuICAgICAgICB9IGVsc2UgaWYgKHBhcmVudE5vZGUuY2hpbGRyZW4uc29tZShjaGlsZCA9PiBjaGlsZC5pc0hhbGZDaGVja2VkIHx8IGNoaWxkLmlzQ2hlY2tlZCkpIHtcbiAgICAgICAgICBwYXJlbnROb2RlLmlzQ2hlY2tlZCA9IGZhbHNlO1xuICAgICAgICAgIHBhcmVudE5vZGUuaXNIYWxmQ2hlY2tlZCA9IHRydWU7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgcGFyZW50Tm9kZS5pc0NoZWNrZWQgPSBmYWxzZTtcbiAgICAgICAgICBwYXJlbnROb2RlLmlzSGFsZkNoZWNrZWQgPSBmYWxzZTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgICAgdGhpcy5zZXRDaGVja2VkTm9kZUxpc3QocGFyZW50Tm9kZSk7XG4gICAgICB0aGlzLnNldEhhbGZDaGVja2VkTm9kZUxpc3QocGFyZW50Tm9kZSk7XG4gICAgICB0aGlzLmNvbmR1Y3RVcChwYXJlbnROb2RlKTtcbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogcmVzZXQgY2hpbGQgY2hlY2sgc3RhdGVcbiAgICovXG4gIGNvbmR1Y3REb3duKG5vZGU6IE56VHJlZU5vZGUsIHZhbHVlOiBib29sZWFuKTogdm9pZCB7XG4gICAgaWYgKCFpc0NoZWNrRGlzYWJsZWQobm9kZSkpIHtcbiAgICAgIG5vZGUuaXNDaGVja2VkID0gdmFsdWU7XG4gICAgICBub2RlLmlzSGFsZkNoZWNrZWQgPSBmYWxzZTtcbiAgICAgIHRoaXMuc2V0Q2hlY2tlZE5vZGVMaXN0KG5vZGUpO1xuICAgICAgdGhpcy5zZXRIYWxmQ2hlY2tlZE5vZGVMaXN0KG5vZGUpO1xuICAgICAgbm9kZS5jaGlsZHJlbi5mb3JFYWNoKG4gPT4ge1xuICAgICAgICB0aGlzLmNvbmR1Y3REb3duKG4sIHZhbHVlKTtcbiAgICAgIH0pO1xuICAgIH1cbiAgfVxuXG4gIC8qKlxuICAgKiBzZWFyY2ggdmFsdWUgJiBleHBhbmQgbm9kZVxuICAgKiBzaG91bGQgYWRkIGV4cGFuZGxpc3RcbiAgICovXG4gIHNlYXJjaEV4cGFuZCh2YWx1ZTogc3RyaW5nKTogdm9pZCB7XG4gICAgdGhpcy5tYXRjaGVkTm9kZUxpc3QgPSBbXTtcbiAgICBjb25zdCBleHBhbmRlZEtleXM6IHN0cmluZ1tdID0gW107XG4gICAgaWYgKCFpc05vdE5pbCh2YWx1ZSkpIHtcbiAgICAgIHJldHVybjtcbiAgICB9XG4gICAgLy8gdG8gcmVzZXQgZXhwYW5kZWROb2RlTGlzdFxuICAgIGNvbnN0IGV4cGFuZFBhcmVudCA9IChuOiBOelRyZWVOb2RlKSA9PiB7XG4gICAgICAvLyBleHBhbmQgcGFyZW50IG5vZGVcbiAgICAgIGNvbnN0IHBhcmVudE5vZGUgPSBuLmdldFBhcmVudE5vZGUoKTtcbiAgICAgIGlmIChwYXJlbnROb2RlKSB7XG4gICAgICAgIGV4cGFuZGVkS2V5cy5wdXNoKHBhcmVudE5vZGUua2V5KTtcbiAgICAgICAgZXhwYW5kUGFyZW50KHBhcmVudE5vZGUpO1xuICAgICAgfVxuICAgIH07XG4gICAgY29uc3Qgc2VhcmNoQ2hpbGQgPSAobjogTnpUcmVlTm9kZSkgPT4ge1xuICAgICAgaWYgKHZhbHVlICYmIG4udGl0bGUuaW5jbHVkZXModmFsdWUpKSB7XG4gICAgICAgIC8vIG1hdGNoIHRoZSBub2RlXG4gICAgICAgIG4uaXNNYXRjaGVkID0gdHJ1ZTtcbiAgICAgICAgdGhpcy5tYXRjaGVkTm9kZUxpc3QucHVzaChuKTtcbiAgICAgICAgLy8gZXhwYW5kIHBhcmVudE5vZGVcbiAgICAgICAgZXhwYW5kUGFyZW50KG4pO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgbi5pc01hdGNoZWQgPSBmYWxzZTtcbiAgICAgIH1cbiAgICAgIG4uY2hpbGRyZW4uZm9yRWFjaChjaGlsZCA9PiB7XG4gICAgICAgIHNlYXJjaENoaWxkKGNoaWxkKTtcbiAgICAgIH0pO1xuICAgIH07XG4gICAgdGhpcy5yb290Tm9kZXMuZm9yRWFjaChjaGlsZCA9PiB7XG4gICAgICBzZWFyY2hDaGlsZChjaGlsZCk7XG4gICAgfSk7XG4gICAgLy8gZXhwYW5kIG1hdGNoZWQga2V5c1xuICAgIHRoaXMuY2FsY0V4cGFuZGVkS2V5cyhleHBhbmRlZEtleXMsIHRoaXMucm9vdE5vZGVzKTtcbiAgfVxuXG4gIC8qKlxuICAgKiBmbHVzaCBhZnRlciBkZWxldGUgbm9kZVxuICAgKi9cbiAgYWZ0ZXJSZW1vdmUobm9kZXM6IE56VHJlZU5vZGVbXSk6IHZvaWQge1xuICAgIC8vIHRvIHJlc2V0IHNlbGVjdGVkTm9kZUxpc3QgJiBleHBhbmRlZE5vZGVMaXN0XG4gICAgY29uc3QgbG9vcE5vZGUgPSAobm9kZTogTnpUcmVlTm9kZSkgPT4ge1xuICAgICAgLy8gcmVtb3ZlIHNlbGVjdGVkIG5vZGVcbiAgICAgIHRoaXMuc2VsZWN0ZWROb2RlTGlzdCA9IHRoaXMuc2VsZWN0ZWROb2RlTGlzdC5maWx0ZXIobiA9PiBuLmtleSAhPT0gbm9kZS5rZXkpO1xuICAgICAgLy8gcmVtb3ZlIGV4cGFuZGVkIG5vZGVcbiAgICAgIHRoaXMuZXhwYW5kZWROb2RlTGlzdCA9IHRoaXMuZXhwYW5kZWROb2RlTGlzdC5maWx0ZXIobiA9PiBuLmtleSAhPT0gbm9kZS5rZXkpO1xuICAgICAgLy8gcmVtb3ZlIGNoZWNrZWQgbm9kZVxuICAgICAgdGhpcy5jaGVja2VkTm9kZUxpc3QgPSB0aGlzLmNoZWNrZWROb2RlTGlzdC5maWx0ZXIobiA9PiBuLmtleSAhPT0gbm9kZS5rZXkpO1xuICAgICAgaWYgKG5vZGUuY2hpbGRyZW4pIHtcbiAgICAgICAgbm9kZS5jaGlsZHJlbi5mb3JFYWNoKGNoaWxkID0+IHtcbiAgICAgICAgICBsb29wTm9kZShjaGlsZCk7XG4gICAgICAgIH0pO1xuICAgICAgfVxuICAgIH07XG4gICAgbm9kZXMuZm9yRWFjaChuID0+IHtcbiAgICAgIGxvb3BOb2RlKG4pO1xuICAgIH0pO1xuICAgIHRoaXMucmVmcmVzaENoZWNrU3RhdGUodGhpcy5pc0NoZWNrU3RyaWN0bHkpO1xuICB9XG5cbiAgLyoqXG4gICAqIGRyYWcgZXZlbnRcbiAgICovXG4gIHJlZnJlc2hEcmFnTm9kZShub2RlOiBOelRyZWVOb2RlKTogdm9pZCB7XG4gICAgaWYgKG5vZGUuY2hpbGRyZW4ubGVuZ3RoID09PSAwKSB7XG4gICAgICAvLyB1bnRpbCByb290XG4gICAgICB0aGlzLmNvbmR1Y3RVcChub2RlKTtcbiAgICB9IGVsc2Uge1xuICAgICAgbm9kZS5jaGlsZHJlbi5mb3JFYWNoKChjaGlsZCkgPT4ge1xuICAgICAgICB0aGlzLnJlZnJlc2hEcmFnTm9kZShjaGlsZCk7XG4gICAgICB9KTtcbiAgICB9XG4gIH1cblxuICAvLyByZXNldCBub2RlIGxldmVsXG4gIHJlc2V0Tm9kZUxldmVsKG5vZGU6IE56VHJlZU5vZGUpOiB2b2lkIHtcbiAgICBjb25zdCBwYXJlbnROb2RlID0gbm9kZS5nZXRQYXJlbnROb2RlKCk7XG4gICAgaWYgKHBhcmVudE5vZGUpIHtcbiAgICAgIG5vZGUubGV2ZWwgPSBwYXJlbnROb2RlLmxldmVsICsgMTtcbiAgICB9IGVsc2Uge1xuICAgICAgbm9kZS5sZXZlbCA9IDA7XG4gICAgfVxuICAgIGZvciAoY29uc3QgY2hpbGQgb2Ygbm9kZS5jaGlsZHJlbikge1xuICAgICAgdGhpcy5yZXNldE5vZGVMZXZlbChjaGlsZCk7XG4gICAgfVxuICB9XG5cbiAgY2FsY0Ryb3BQb3NpdGlvbihldmVudDogRHJhZ0V2ZW50KTogbnVtYmVyIHtcbiAgICBjb25zdCB7IGNsaWVudFkgfSA9IGV2ZW50O1xuICAgIC8vIHRvIGZpeCBmaXJlZm94IHVuZGVmaW5lZFxuICAgIGNvbnN0IHsgdG9wLCBib3R0b20sIGhlaWdodCB9ID0gZXZlbnQuc3JjRWxlbWVudCA/IGV2ZW50LnNyY0VsZW1lbnQuZ2V0Qm91bmRpbmdDbGllbnRSZWN0KCkgOiAoZXZlbnQudGFyZ2V0IGFzIEVsZW1lbnQpLmdldEJvdW5kaW5nQ2xpZW50UmVjdCgpO1xuICAgIGNvbnN0IGRlcyA9IE1hdGgubWF4KGhlaWdodCAqIHRoaXMuRFJBR19TSURFX1JBTkdFLCB0aGlzLkRSQUdfTUlOX0dBUCk7XG5cbiAgICBpZiAoY2xpZW50WSA8PSB0b3AgKyBkZXMpIHtcbiAgICAgIHJldHVybiAtMTtcbiAgICB9IGVsc2UgaWYgKGNsaWVudFkgPj0gYm90dG9tIC0gZGVzKSB7XG4gICAgICByZXR1cm4gMTtcbiAgICB9XG5cbiAgICByZXR1cm4gMDtcbiAgfVxuXG4gIC8qKlxuICAgKiBkcm9wXG4gICAqIDA6IGlubmVyIC0xOiBwcmUgMTogbmV4dFxuICAgKi9cbiAgZHJvcEFuZEFwcGx5KHRhcmdldE5vZGU6IE56VHJlZU5vZGUsIGRyYWdQb3M6IG51bWJlciA9IC0xKTogdm9pZCB7XG4gICAgaWYgKCF0YXJnZXROb2RlIHx8IGRyYWdQb3MgPiAxKSB7XG4gICAgICByZXR1cm47XG4gICAgfVxuICAgIGNvbnN0IHRyZWVTZXJ2aWNlID0gdGFyZ2V0Tm9kZS50cmVlU2VydmljZTtcbiAgICBjb25zdCB0YXJnZXRQYXJlbnQgPSB0YXJnZXROb2RlLmdldFBhcmVudE5vZGUoKTtcbiAgICBjb25zdCBpc1NlbGVjdGVkUm9vdE5vZGUgPSB0aGlzLnNlbGVjdGVkTm9kZS5nZXRQYXJlbnROb2RlKCk7XG4gICAgLy8gcmVtb3ZlIHRoZSBkcmFnTm9kZVxuICAgIGlmIChpc1NlbGVjdGVkUm9vdE5vZGUpIHtcbiAgICAgIGlzU2VsZWN0ZWRSb290Tm9kZS5jaGlsZHJlbiA9IGlzU2VsZWN0ZWRSb290Tm9kZS5jaGlsZHJlbi5maWx0ZXIobiA9PiBuLmtleSAhPT0gdGhpcy5zZWxlY3RlZE5vZGUua2V5KTtcbiAgICB9IGVsc2Uge1xuICAgICAgdGhpcy5yb290Tm9kZXMgPSB0aGlzLnJvb3ROb2Rlcy5maWx0ZXIobiA9PiBuLmtleSAhPT0gdGhpcy5zZWxlY3RlZE5vZGUua2V5KTtcbiAgICB9XG4gICAgc3dpdGNoIChkcmFnUG9zKSB7XG4gICAgICBjYXNlIDA6XG4gICAgICAgIHRhcmdldE5vZGUuYWRkQ2hpbGRyZW4oWyB0aGlzLnNlbGVjdGVkTm9kZSBdKTtcbiAgICAgICAgdGhpcy5yZXNldE5vZGVMZXZlbCh0YXJnZXROb2RlKTtcbiAgICAgICAgYnJlYWs7XG4gICAgICBjYXNlIC0xOlxuICAgICAgY2FzZSAxOlxuICAgICAgICBjb25zdCB0SW5kZXggPSBkcmFnUG9zID09PSAxID8gMSA6IDA7XG4gICAgICAgIGlmICh0YXJnZXRQYXJlbnQpIHtcbiAgICAgICAgICB0YXJnZXRQYXJlbnQuYWRkQ2hpbGRyZW4oWyB0aGlzLnNlbGVjdGVkTm9kZSBdLCB0YXJnZXRQYXJlbnQuY2hpbGRyZW4uaW5kZXhPZih0YXJnZXROb2RlKSArIHRJbmRleCk7XG4gICAgICAgICAgY29uc3QgcGFyZW50Tm9kZSA9IHRoaXMuc2VsZWN0ZWROb2RlLmdldFBhcmVudE5vZGUoKTtcbiAgICAgICAgICBpZiAocGFyZW50Tm9kZSkge1xuICAgICAgICAgICAgdGhpcy5yZXNldE5vZGVMZXZlbChwYXJlbnROb2RlKTtcbiAgICAgICAgICB9XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgY29uc3QgdGFyZ2V0SW5kZXggPSB0aGlzLnJvb3ROb2Rlcy5pbmRleE9mKHRhcmdldE5vZGUpICsgdEluZGV4O1xuICAgICAgICAgIC8vIOagueiKgueCueaPkuWFpVxuICAgICAgICAgIHRoaXMucm9vdE5vZGVzLnNwbGljZSh0YXJnZXRJbmRleCwgMCwgdGhpcy5zZWxlY3RlZE5vZGUpO1xuICAgICAgICAgIHRoaXMucm9vdE5vZGVzWyB0YXJnZXRJbmRleCBdLnBhcmVudE5vZGUgPSBudWxsO1xuICAgICAgICAgIHRoaXMucm9vdE5vZGVzWyB0YXJnZXRJbmRleCBdLmxldmVsID0gMDtcbiAgICAgICAgfVxuICAgICAgICBicmVhaztcbiAgICB9XG4gICAgLy8gZmx1c2ggYWxsIG5vZGVzXG4gICAgdGhpcy5yb290Tm9kZXMuZm9yRWFjaCgoY2hpbGQpID0+IHtcbiAgICAgIGlmICghY2hpbGQudHJlZVNlcnZpY2UpIHtcbiAgICAgICAgY2hpbGQuc2VydmljZSA9IHRyZWVTZXJ2aWNlO1xuICAgICAgfVxuICAgICAgdGhpcy5yZWZyZXNoRHJhZ05vZGUoY2hpbGQpO1xuICAgIH0pO1xuICB9XG5cbiAgLyoqXG4gICAqIGVtaXQgU3RydWN0dXJlXG4gICAqIGV2ZW50TmFtZVxuICAgKiBub2RlXG4gICAqIGV2ZW50OiBNb3VzZUV2ZW50IC8gRHJhZ0V2ZW50XG4gICAqIGRyYWdOb2RlXG4gICAqL1xuICBmb3JtYXRFdmVudChldmVudE5hbWU6IHN0cmluZywgbm9kZTogTnpUcmVlTm9kZSB8IG51bGwsIGV2ZW50OiBNb3VzZUV2ZW50IHwgRHJhZ0V2ZW50IHwgbnVsbCk6IE56Rm9ybWF0RW1pdEV2ZW50IHtcbiAgICBjb25zdCBlbWl0U3RydWN0dXJlOiBOekZvcm1hdEVtaXRFdmVudCA9IHtcbiAgICAgICdldmVudE5hbWUnOiBldmVudE5hbWUsXG4gICAgICAnbm9kZScgICAgIDogbm9kZSxcbiAgICAgICdldmVudCcgICAgOiBldmVudFxuICAgIH07XG4gICAgc3dpdGNoIChldmVudE5hbWUpIHtcbiAgICAgIGNhc2UgJ2RyYWdzdGFydCc6XG4gICAgICBjYXNlICdkcmFnZW50ZXInOlxuICAgICAgY2FzZSAnZHJhZ292ZXInOlxuICAgICAgY2FzZSAnZHJhZ2xlYXZlJzpcbiAgICAgIGNhc2UgJ2Ryb3AnOlxuICAgICAgY2FzZSAnZHJhZ2VuZCc6XG4gICAgICAgIE9iamVjdC5hc3NpZ24oZW1pdFN0cnVjdHVyZSwgeyAnZHJhZ05vZGUnOiB0aGlzLmdldFNlbGVjdGVkTm9kZSgpIH0pO1xuICAgICAgICBicmVhaztcbiAgICAgIGNhc2UgJ2NsaWNrJzpcbiAgICAgIGNhc2UgJ2RibGNsaWNrJzpcbiAgICAgICAgT2JqZWN0LmFzc2lnbihlbWl0U3RydWN0dXJlLCB7ICdzZWxlY3RlZEtleXMnOiB0aGlzLnNlbGVjdGVkTm9kZUxpc3QgfSk7XG4gICAgICAgIE9iamVjdC5hc3NpZ24oZW1pdFN0cnVjdHVyZSwgeyAnbm9kZXMnOiB0aGlzLnNlbGVjdGVkTm9kZUxpc3QgfSk7XG4gICAgICAgIE9iamVjdC5hc3NpZ24oZW1pdFN0cnVjdHVyZSwgeyAna2V5cyc6IHRoaXMuc2VsZWN0ZWROb2RlTGlzdC5tYXAobiA9PiBuLmtleSkgfSk7XG4gICAgICAgIGJyZWFrO1xuICAgICAgY2FzZSAnY2hlY2snOlxuICAgICAgICBjb25zdCBjaGVja2VkTm9kZUxpc3QgPSB0aGlzLmdldENoZWNrZWROb2RlTGlzdCgpO1xuXG4gICAgICAgIE9iamVjdC5hc3NpZ24oZW1pdFN0cnVjdHVyZSwgeyAnY2hlY2tlZEtleXMnOiBjaGVja2VkTm9kZUxpc3QgfSk7XG4gICAgICAgIE9iamVjdC5hc3NpZ24oZW1pdFN0cnVjdHVyZSwgeyAnbm9kZXMnOiBjaGVja2VkTm9kZUxpc3QgfSk7XG4gICAgICAgIE9iamVjdC5hc3NpZ24oZW1pdFN0cnVjdHVyZSwgeyAna2V5cyc6IGNoZWNrZWROb2RlTGlzdC5tYXAobiA9PiBuLmtleSkgfSk7XG4gICAgICAgIGJyZWFrO1xuICAgICAgY2FzZSAnc2VhcmNoJzpcbiAgICAgICAgT2JqZWN0LmFzc2lnbihlbWl0U3RydWN0dXJlLCB7ICdtYXRjaGVkS2V5cyc6IHRoaXMuZ2V0TWF0Y2hlZE5vZGVMaXN0KCkgfSk7XG4gICAgICAgIE9iamVjdC5hc3NpZ24oZW1pdFN0cnVjdHVyZSwgeyAnbm9kZXMnOiB0aGlzLmdldE1hdGNoZWROb2RlTGlzdCgpIH0pO1xuICAgICAgICBPYmplY3QuYXNzaWduKGVtaXRTdHJ1Y3R1cmUsIHsgJ2tleXMnOiB0aGlzLmdldE1hdGNoZWROb2RlTGlzdCgpLm1hcChuID0+IG4ua2V5KSB9KTtcbiAgICAgICAgYnJlYWs7XG4gICAgICBjYXNlICdleHBhbmQnOlxuICAgICAgICBPYmplY3QuYXNzaWduKGVtaXRTdHJ1Y3R1cmUsIHsgJ25vZGVzJzogdGhpcy5leHBhbmRlZE5vZGVMaXN0IH0pO1xuICAgICAgICBPYmplY3QuYXNzaWduKGVtaXRTdHJ1Y3R1cmUsIHsgJ2tleXMnOiB0aGlzLmV4cGFuZGVkTm9kZUxpc3QubWFwKG4gPT4gbi5rZXkpIH0pO1xuICAgICAgICBicmVhaztcbiAgICB9XG4gICAgcmV0dXJuIGVtaXRTdHJ1Y3R1cmU7XG4gIH1cblxuICBuZ09uRGVzdHJveSgpOiB2b2lkIHtcbiAgICB0aGlzLnRyaWdnZXJFdmVudENoYW5nZSQuY29tcGxldGUoKTtcbiAgfVxufVxuIl19