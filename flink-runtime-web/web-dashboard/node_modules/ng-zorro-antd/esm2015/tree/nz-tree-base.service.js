/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { Injectable } from '@angular/core';
import { Subject } from 'rxjs';
import { isNotNil } from '../core/util/check';
import { NzTreeNode } from './nz-tree-node';
import { isCheckDisabled, isInArray } from './nz-tree-util';
export class NzTreeBaseService {
    constructor() {
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
     * @return {?}
     */
    eventTriggerChanged() {
        return this.triggerEventChange$.asObservable();
    }
    /**
     * reset tree nodes will clear default node list
     * @param {?} nzNodes
     * @return {?}
     */
    initTree(nzNodes) {
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
        () => {
            this.refreshCheckState(this.isCheckStrictly);
        }));
    }
    /**
     * @return {?}
     */
    getSelectedNode() {
        return this.selectedNode;
    }
    /**
     * get some list
     * @return {?}
     */
    getSelectedNodeList() {
        return this.conductNodeState('select');
    }
    /**
     * return checked nodes
     * @return {?}
     */
    getCheckedNodeList() {
        return this.conductNodeState('check');
    }
    /**
     * @return {?}
     */
    getHalfCheckedNodeList() {
        return this.conductNodeState('halfCheck');
    }
    /**
     * return expanded nodes
     * @return {?}
     */
    getExpandedNodeList() {
        return this.conductNodeState('expand');
    }
    /**
     * return search matched nodes
     * @return {?}
     */
    getMatchedNodeList() {
        return this.conductNodeState('match');
    }
    // tslint:disable-next-line:no-any
    /**
     * @param {?} value
     * @return {?}
     */
    isArrayOfNzTreeNode(value) {
        return value.every((/**
         * @param {?} item
         * @return {?}
         */
        item => item instanceof NzTreeNode));
    }
    /**
     * reset selectedNodeList
     * @param {?} selectedKeys
     * @param {?} nzNodes
     * @param {?=} isMulti
     * @return {?}
     */
    calcSelectedKeys(selectedKeys, nzNodes, isMulti = false) {
        /** @type {?} */
        const calc = (/**
         * @param {?} nodes
         * @return {?}
         */
        (nodes) => {
            return nodes.every((/**
             * @param {?} node
             * @return {?}
             */
            node => {
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
    }
    /**
     * reset expandedNodeList
     * @param {?} expandedKeys
     * @param {?} nzNodes
     * @return {?}
     */
    calcExpandedKeys(expandedKeys, nzNodes) {
        this.expandedNodeList = [];
        /** @type {?} */
        const calc = (/**
         * @param {?} nodes
         * @return {?}
         */
        (nodes) => {
            nodes.forEach((/**
             * @param {?} node
             * @return {?}
             */
            node => {
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
    }
    /**
     * reset checkedNodeList
     * @param {?} checkedKeys
     * @param {?} nzNodes
     * @param {?=} isCheckStrictly
     * @return {?}
     */
    calcCheckedKeys(checkedKeys, nzNodes, isCheckStrictly = false) {
        this.checkedNodeList = [];
        this.halfCheckedNodeList = [];
        /** @type {?} */
        const calc = (/**
         * @param {?} nodes
         * @return {?}
         */
        (nodes) => {
            nodes.forEach((/**
             * @param {?} node
             * @return {?}
             */
            node => {
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
    }
    /**
     * set drag node
     * @param {?} node
     * @return {?}
     */
    setSelectedNode(node) {
        this.selectedNode = node;
    }
    /**
     * set node selected status
     * @param {?} node
     * @return {?}
     */
    setNodeActive(node) {
        if (!this.isMultiple && node.isSelected) {
            this.selectedNodeList.forEach((/**
             * @param {?} n
             * @return {?}
             */
            n => {
                if (node.key !== n.key) {
                    // reset other nodes
                    n.isSelected = false;
                }
            }));
            // single mode: remove pre node
            this.selectedNodeList = [];
        }
        this.setSelectedNodeList(node, this.isMultiple);
    }
    /**
     * add or remove node to selectedNodeList
     * @param {?} node
     * @param {?=} isMultiple
     * @return {?}
     */
    setSelectedNodeList(node, isMultiple = false) {
        /** @type {?} */
        const index = this.selectedNodeList.findIndex((/**
         * @param {?} n
         * @return {?}
         */
        n => node.key === n.key));
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
            n => (n.key !== node.key)));
        }
    }
    /**
     * merge checked nodes
     * @param {?} node
     * @return {?}
     */
    setHalfCheckedNodeList(node) {
        /** @type {?} */
        const index = this.halfCheckedNodeList.findIndex((/**
         * @param {?} n
         * @return {?}
         */
        n => node.key === n.key));
        if (node.isHalfChecked && index === -1) {
            this.halfCheckedNodeList.push(node);
        }
        else if (!node.isHalfChecked && index > -1) {
            this.halfCheckedNodeList = this.halfCheckedNodeList.filter((/**
             * @param {?} n
             * @return {?}
             */
            n => node.key !== n.key));
        }
    }
    /**
     * @param {?} node
     * @return {?}
     */
    setCheckedNodeList(node) {
        /** @type {?} */
        const index = this.checkedNodeList.findIndex((/**
         * @param {?} n
         * @return {?}
         */
        n => node.key === n.key));
        if (node.isChecked && index === -1) {
            this.checkedNodeList.push(node);
        }
        else if (!node.isChecked && index > -1) {
            this.checkedNodeList = this.checkedNodeList.filter((/**
             * @param {?} n
             * @return {?}
             */
            n => node.key !== n.key));
        }
    }
    /**
     * conduct checked/selected/expanded keys
     * @param {?=} type
     * @return {?}
     */
    conductNodeState(type = 'check') {
        /** @type {?} */
        let resultNodesList = [];
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
                const isIgnore = (/**
                 * @param {?} node
                 * @return {?}
                 */
                (node) => {
                    /** @type {?} */
                    const parentNode = node.getParentNode();
                    if (parentNode) {
                        if (this.checkedNodeList.findIndex((/**
                         * @param {?} n
                         * @return {?}
                         */
                        n => n.key === parentNode.key)) > -1) {
                            return true;
                        }
                        else {
                            return isIgnore(parentNode);
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
                    n => !isIgnore(n)));
                }
                break;
            case 'halfCheck':
                if (!this.isCheckStrictly) {
                    resultNodesList = this.halfCheckedNodeList;
                }
                break;
        }
        return resultNodesList;
    }
    /**
     * set expanded nodes
     * @param {?} node
     * @return {?}
     */
    setExpandedNodeList(node) {
        if (node.isLeaf) {
            return;
        }
        /** @type {?} */
        const index = this.expandedNodeList.findIndex((/**
         * @param {?} n
         * @return {?}
         */
        n => node.key === n.key));
        if (node.isExpanded && index === -1) {
            this.expandedNodeList.push(node);
        }
        else if (!node.isExpanded && index > -1) {
            this.expandedNodeList = this.expandedNodeList.filter((/**
             * @param {?} n
             * @return {?}
             */
            n => node.key !== n.key));
        }
    }
    /**
     * check state
     * @param {?=} isCheckStrictly
     * @return {?}
     */
    refreshCheckState(isCheckStrictly = false) {
        if (isCheckStrictly) {
            return;
        }
        this.checkedNodeList.forEach((/**
         * @param {?} node
         * @return {?}
         */
        node => {
            this.conduct(node);
        }));
    }
    // reset other node checked state based current node
    /**
     * @param {?} node
     * @return {?}
     */
    conduct(node) {
        /** @type {?} */
        const isChecked = node.isChecked;
        if (node) {
            this.conductUp(node);
            this.conductDown(node, isChecked);
        }
    }
    /**
     * 1、children half checked
     * 2、children all checked, parent checked
     * 3、no children checked
     * @param {?} node
     * @return {?}
     */
    conductUp(node) {
        /** @type {?} */
        const parentNode = node.getParentNode();
        // 全禁用节点不选中
        if (parentNode) {
            if (!isCheckDisabled(parentNode)) {
                if (parentNode.children.every((/**
                 * @param {?} child
                 * @return {?}
                 */
                child => isCheckDisabled(child) || (!child.isHalfChecked && child.isChecked)))) {
                    parentNode.isChecked = true;
                    parentNode.isHalfChecked = false;
                }
                else if (parentNode.children.some((/**
                 * @param {?} child
                 * @return {?}
                 */
                child => child.isHalfChecked || child.isChecked))) {
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
    }
    /**
     * reset child check state
     * @param {?} node
     * @param {?} value
     * @return {?}
     */
    conductDown(node, value) {
        if (!isCheckDisabled(node)) {
            node.isChecked = value;
            node.isHalfChecked = false;
            this.setCheckedNodeList(node);
            this.setHalfCheckedNodeList(node);
            node.children.forEach((/**
             * @param {?} n
             * @return {?}
             */
            n => {
                this.conductDown(n, value);
            }));
        }
    }
    /**
     * search value & expand node
     * should add expandlist
     * @param {?} value
     * @return {?}
     */
    searchExpand(value) {
        this.matchedNodeList = [];
        /** @type {?} */
        const expandedKeys = [];
        if (!isNotNil(value)) {
            return;
        }
        // to reset expandedNodeList
        /** @type {?} */
        const expandParent = (/**
         * @param {?} n
         * @return {?}
         */
        (n) => {
            // expand parent node
            /** @type {?} */
            const parentNode = n.getParentNode();
            if (parentNode) {
                expandedKeys.push(parentNode.key);
                expandParent(parentNode);
            }
        });
        /** @type {?} */
        const searchChild = (/**
         * @param {?} n
         * @return {?}
         */
        (n) => {
            if (value && n.title.includes(value)) {
                // match the node
                n.isMatched = true;
                this.matchedNodeList.push(n);
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
            child => {
                searchChild(child);
            }));
        });
        this.rootNodes.forEach((/**
         * @param {?} child
         * @return {?}
         */
        child => {
            searchChild(child);
        }));
        // expand matched keys
        this.calcExpandedKeys(expandedKeys, this.rootNodes);
    }
    /**
     * flush after delete node
     * @param {?} nodes
     * @return {?}
     */
    afterRemove(nodes) {
        // to reset selectedNodeList & expandedNodeList
        /** @type {?} */
        const loopNode = (/**
         * @param {?} node
         * @return {?}
         */
        (node) => {
            // remove selected node
            this.selectedNodeList = this.selectedNodeList.filter((/**
             * @param {?} n
             * @return {?}
             */
            n => n.key !== node.key));
            // remove expanded node
            this.expandedNodeList = this.expandedNodeList.filter((/**
             * @param {?} n
             * @return {?}
             */
            n => n.key !== node.key));
            // remove checked node
            this.checkedNodeList = this.checkedNodeList.filter((/**
             * @param {?} n
             * @return {?}
             */
            n => n.key !== node.key));
            if (node.children) {
                node.children.forEach((/**
                 * @param {?} child
                 * @return {?}
                 */
                child => {
                    loopNode(child);
                }));
            }
        });
        nodes.forEach((/**
         * @param {?} n
         * @return {?}
         */
        n => {
            loopNode(n);
        }));
        this.refreshCheckState(this.isCheckStrictly);
    }
    /**
     * drag event
     * @param {?} node
     * @return {?}
     */
    refreshDragNode(node) {
        if (node.children.length === 0) {
            // until root
            this.conductUp(node);
        }
        else {
            node.children.forEach((/**
             * @param {?} child
             * @return {?}
             */
            (child) => {
                this.refreshDragNode(child);
            }));
        }
    }
    // reset node level
    /**
     * @param {?} node
     * @return {?}
     */
    resetNodeLevel(node) {
        /** @type {?} */
        const parentNode = node.getParentNode();
        if (parentNode) {
            node.level = parentNode.level + 1;
        }
        else {
            node.level = 0;
        }
        for (const child of node.children) {
            this.resetNodeLevel(child);
        }
    }
    /**
     * @param {?} event
     * @return {?}
     */
    calcDropPosition(event) {
        const { clientY } = event;
        // to fix firefox undefined
        const { top, bottom, height } = event.srcElement ? event.srcElement.getBoundingClientRect() : ((/** @type {?} */ (event.target))).getBoundingClientRect();
        /** @type {?} */
        const des = Math.max(height * this.DRAG_SIDE_RANGE, this.DRAG_MIN_GAP);
        if (clientY <= top + des) {
            return -1;
        }
        else if (clientY >= bottom - des) {
            return 1;
        }
        return 0;
    }
    /**
     * drop
     * 0: inner -1: pre 1: next
     * @param {?} targetNode
     * @param {?=} dragPos
     * @return {?}
     */
    dropAndApply(targetNode, dragPos = -1) {
        if (!targetNode || dragPos > 1) {
            return;
        }
        /** @type {?} */
        const treeService = targetNode.treeService;
        /** @type {?} */
        const targetParent = targetNode.getParentNode();
        /** @type {?} */
        const isSelectedRootNode = this.selectedNode.getParentNode();
        // remove the dragNode
        if (isSelectedRootNode) {
            isSelectedRootNode.children = isSelectedRootNode.children.filter((/**
             * @param {?} n
             * @return {?}
             */
            n => n.key !== this.selectedNode.key));
        }
        else {
            this.rootNodes = this.rootNodes.filter((/**
             * @param {?} n
             * @return {?}
             */
            n => n.key !== this.selectedNode.key));
        }
        switch (dragPos) {
            case 0:
                targetNode.addChildren([this.selectedNode]);
                this.resetNodeLevel(targetNode);
                break;
            case -1:
            case 1:
                /** @type {?} */
                const tIndex = dragPos === 1 ? 1 : 0;
                if (targetParent) {
                    targetParent.addChildren([this.selectedNode], targetParent.children.indexOf(targetNode) + tIndex);
                    /** @type {?} */
                    const parentNode = this.selectedNode.getParentNode();
                    if (parentNode) {
                        this.resetNodeLevel(parentNode);
                    }
                }
                else {
                    /** @type {?} */
                    const targetIndex = this.rootNodes.indexOf(targetNode) + tIndex;
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
        (child) => {
            if (!child.treeService) {
                child.service = treeService;
            }
            this.refreshDragNode(child);
        }));
    }
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
    formatEvent(eventName, node, event) {
        /** @type {?} */
        const emitStructure = {
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
                    n => n.key)) });
                break;
            case 'check':
                /** @type {?} */
                const checkedNodeList = this.getCheckedNodeList();
                Object.assign(emitStructure, { 'checkedKeys': checkedNodeList });
                Object.assign(emitStructure, { 'nodes': checkedNodeList });
                Object.assign(emitStructure, { 'keys': checkedNodeList.map((/**
                     * @param {?} n
                     * @return {?}
                     */
                    n => n.key)) });
                break;
            case 'search':
                Object.assign(emitStructure, { 'matchedKeys': this.getMatchedNodeList() });
                Object.assign(emitStructure, { 'nodes': this.getMatchedNodeList() });
                Object.assign(emitStructure, { 'keys': this.getMatchedNodeList().map((/**
                     * @param {?} n
                     * @return {?}
                     */
                    n => n.key)) });
                break;
            case 'expand':
                Object.assign(emitStructure, { 'nodes': this.expandedNodeList });
                Object.assign(emitStructure, { 'keys': this.expandedNodeList.map((/**
                     * @param {?} n
                     * @return {?}
                     */
                    n => n.key)) });
                break;
        }
        return emitStructure;
    }
    /**
     * @return {?}
     */
    ngOnDestroy() {
        this.triggerEventChange$.complete();
    }
}
NzTreeBaseService.decorators = [
    { type: Injectable }
];
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotdHJlZS1iYXNlLnNlcnZpY2UuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsidHJlZS9uei10cmVlLWJhc2Uuc2VydmljZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUFFLFVBQVUsRUFBYSxNQUFNLGVBQWUsQ0FBQztBQUN0RCxPQUFPLEVBQWMsT0FBTyxFQUFFLE1BQU0sTUFBTSxDQUFDO0FBQzNDLE9BQU8sRUFBRSxRQUFRLEVBQUUsTUFBTSxvQkFBb0IsQ0FBQztBQUU5QyxPQUFPLEVBQUUsVUFBVSxFQUFFLE1BQU0sZ0JBQWdCLENBQUM7QUFDNUMsT0FBTyxFQUFFLGVBQWUsRUFBRSxTQUFTLEVBQUUsTUFBTSxnQkFBZ0IsQ0FBQztBQUc1RCxNQUFNLE9BQU8saUJBQWlCO0lBRDlCO1FBRUUsb0JBQWUsR0FBRyxJQUFJLENBQUM7UUFDdkIsaUJBQVksR0FBRyxDQUFDLENBQUM7UUFFakIsb0JBQWUsR0FBWSxLQUFLLENBQUM7UUFDakMsZUFBVSxHQUFZLEtBQUssQ0FBQztRQUU1QixjQUFTLEdBQWlCLEVBQUUsQ0FBQztRQUM3QixxQkFBZ0IsR0FBaUIsRUFBRSxDQUFDO1FBQ3BDLHFCQUFnQixHQUFpQixFQUFFLENBQUM7UUFDcEMsb0JBQWUsR0FBaUIsRUFBRSxDQUFDO1FBQ25DLHdCQUFtQixHQUFpQixFQUFFLENBQUM7UUFDdkMsb0JBQWUsR0FBaUIsRUFBRSxDQUFDO1FBQ25DLHdCQUFtQixHQUFHLElBQUksT0FBTyxFQUFxQixDQUFDO0lBK2dCekQsQ0FBQzs7Ozs7SUExZ0JDLG1CQUFtQjtRQUNqQixPQUFPLElBQUksQ0FBQyxtQkFBbUIsQ0FBQyxZQUFZLEVBQUUsQ0FBQztJQUNqRCxDQUFDOzs7Ozs7SUFLRCxRQUFRLENBQUMsT0FBcUI7UUFDNUIsSUFBSSxDQUFDLFNBQVMsR0FBRyxPQUFPLENBQUM7UUFDekIsSUFBSSxDQUFDLGdCQUFnQixHQUFHLEVBQUUs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RUFBRSxFQUFFLE9BQU8sRUFBRSxJQUFJLENBQUMsZ0JBQWdCLEVBQUUsQ0FBQyxDQUFDO2dCQUNqRSxNQUFNLENBQUMsTUFBTSxDQUFDLGFBQWEsRUFBRSxFQUFFLE1BQU0sRUFBRSxJQUFJLENBQUMsZ0JBQWdCLENBQUMsR0FBRzs7OztvQkFBQyxDQUFDLENBQUMsRUFBRSxDQUFDLENBQUMsQ0FBQyxHQUFHLEVBQUMsRUFBRSxDQUFDLENBQUM7Z0JBQ2hGLE1BQU07U0FDVDtRQUNELE9BQU8sYUFBYSxDQUFDO0lBQ3ZCLENBQUM7Ozs7SUFFRCxXQUFXO1FBQ1QsSUFBSSxDQUFDLG1CQUFtQixDQUFDLFFBQVEsRUFBRSxDQUFDO0lBQ3RDLENBQUM7OztZQTVoQkYsVUFBVTs7OztJQUVULDRDQUF1Qjs7SUFDdkIseUNBQWlCOztJQUVqQiw0Q0FBaUM7O0lBQ2pDLHVDQUE0Qjs7SUFDNUIseUNBQXlCOztJQUN6QixzQ0FBNkI7O0lBQzdCLDZDQUFvQzs7SUFDcEMsNkNBQW9DOztJQUNwQyw0Q0FBbUM7O0lBQ25DLGdEQUF1Qzs7SUFDdkMsNENBQW1DOztJQUNuQyxnREFBdUQiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgeyBJbmplY3RhYmxlLCBPbkRlc3Ryb3kgfSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7IE9ic2VydmFibGUsIFN1YmplY3QgfSBmcm9tICdyeGpzJztcbmltcG9ydCB7IGlzTm90TmlsIH0gZnJvbSAnLi4vY29yZS91dGlsL2NoZWNrJztcbmltcG9ydCB7IE56Rm9ybWF0RW1pdEV2ZW50IH0gZnJvbSAnLi9pbnRlcmZhY2UnO1xuaW1wb3J0IHsgTnpUcmVlTm9kZSB9IGZyb20gJy4vbnotdHJlZS1ub2RlJztcbmltcG9ydCB7IGlzQ2hlY2tEaXNhYmxlZCwgaXNJbkFycmF5IH0gZnJvbSAnLi9uei10cmVlLXV0aWwnO1xuXG5ASW5qZWN0YWJsZSgpXG5leHBvcnQgY2xhc3MgTnpUcmVlQmFzZVNlcnZpY2UgaW1wbGVtZW50cyBPbkRlc3Ryb3kge1xuICBEUkFHX1NJREVfUkFOR0UgPSAwLjI1O1xuICBEUkFHX01JTl9HQVAgPSAyO1xuXG4gIGlzQ2hlY2tTdHJpY3RseTogYm9vbGVhbiA9IGZhbHNlO1xuICBpc011bHRpcGxlOiBib29sZWFuID0gZmFsc2U7XG4gIHNlbGVjdGVkTm9kZTogTnpUcmVlTm9kZTtcbiAgcm9vdE5vZGVzOiBOelRyZWVOb2RlW10gPSBbXTtcbiAgc2VsZWN0ZWROb2RlTGlzdDogTnpUcmVlTm9kZVtdID0gW107XG4gIGV4cGFuZGVkTm9kZUxpc3Q6IE56VHJlZU5vZGVbXSA9IFtdO1xuICBjaGVja2VkTm9kZUxpc3Q6IE56VHJlZU5vZGVbXSA9IFtdO1xuICBoYWxmQ2hlY2tlZE5vZGVMaXN0OiBOelRyZWVOb2RlW10gPSBbXTtcbiAgbWF0Y2hlZE5vZGVMaXN0OiBOelRyZWVOb2RlW10gPSBbXTtcbiAgdHJpZ2dlckV2ZW50Q2hhbmdlJCA9IG5ldyBTdWJqZWN0PE56Rm9ybWF0RW1pdEV2ZW50PigpO1xuXG4gIC8qKlxuICAgKiB0cmlnZ2VyIGV2ZW50XG4gICAqL1xuICBldmVudFRyaWdnZXJDaGFuZ2VkKCk6IE9ic2VydmFibGU8TnpGb3JtYXRFbWl0RXZlbnQ+IHtcbiAgICByZXR1cm4gdGhpcy50cmlnZ2VyRXZlbnRDaGFuZ2UkLmFzT2JzZXJ2YWJsZSgpO1xuICB9XG5cbiAgLyoqXG4gICAqIHJlc2V0IHRyZWUgbm9kZXMgd2lsbCBjbGVhciBkZWZhdWx0IG5vZGUgbGlzdFxuICAgKi9cbiAgaW5pdFRyZWUobnpOb2RlczogTnpUcmVlTm9kZVtdKTogdm9pZCB7XG4gICAgdGhpcy5yb290Tm9kZXMgPSBuek5vZGVzO1xuICAgIHRoaXMuZXhwYW5kZWROb2RlTGlzdCA9IFtdO1xuICAgIHRoaXMuc2VsZWN0ZWROb2RlTGlzdCA9IFtdO1xuICAgIHRoaXMuaGFsZkNoZWNrZWROb2RlTGlzdCA9IFtdO1xuICAgIHRoaXMuY2hlY2tlZE5vZGVMaXN0ID0gW107XG4gICAgdGhpcy5tYXRjaGVkTm9kZUxpc3QgPSBbXTtcbiAgICAvLyByZWZyZXNoIG5vZGUgY2hlY2tlZCBzdGF0ZVxuICAgIHNldFRpbWVvdXQoKCkgPT4ge1xuICAgICAgdGhpcy5yZWZyZXNoQ2hlY2tTdGF0ZSh0aGlzLmlzQ2hlY2tTdHJpY3RseSk7XG4gICAgfSk7XG4gIH1cblxuICBnZXRTZWxlY3RlZE5vZGUoKTogTnpUcmVlTm9kZSB8IG51bGwge1xuICAgIHJldHVybiB0aGlzLnNlbGVjdGVkTm9kZTtcbiAgfVxuXG4gIC8qKlxuICAgKiBnZXQgc29tZSBsaXN0XG4gICAqL1xuICBnZXRTZWxlY3RlZE5vZGVMaXN0KCk6IE56VHJlZU5vZGVbXSB7XG4gICAgcmV0dXJuIHRoaXMuY29uZHVjdE5vZGVTdGF0ZSgnc2VsZWN0Jyk7XG4gIH1cblxuICAvKipcbiAgICogcmV0dXJuIGNoZWNrZWQgbm9kZXNcbiAgICovXG4gIGdldENoZWNrZWROb2RlTGlzdCgpOiBOelRyZWVOb2RlW10ge1xuICAgIHJldHVybiB0aGlzLmNvbmR1Y3ROb2RlU3RhdGUoJ2NoZWNrJyk7XG4gIH1cblxuICBnZXRIYWxmQ2hlY2tlZE5vZGVMaXN0KCk6IE56VHJlZU5vZGVbXSB7XG4gICAgcmV0dXJuIHRoaXMuY29uZHVjdE5vZGVTdGF0ZSgnaGFsZkNoZWNrJyk7XG4gIH1cblxuICAvKipcbiAgICogcmV0dXJuIGV4cGFuZGVkIG5vZGVzXG4gICAqL1xuICBnZXRFeHBhbmRlZE5vZGVMaXN0KCk6IE56VHJlZU5vZGVbXSB7XG4gICAgcmV0dXJuIHRoaXMuY29uZHVjdE5vZGVTdGF0ZSgnZXhwYW5kJyk7XG4gIH1cblxuICAvKipcbiAgICogcmV0dXJuIHNlYXJjaCBtYXRjaGVkIG5vZGVzXG4gICAqL1xuICBnZXRNYXRjaGVkTm9kZUxpc3QoKTogTnpUcmVlTm9kZVtdIHtcbiAgICByZXR1cm4gdGhpcy5jb25kdWN0Tm9kZVN0YXRlKCdtYXRjaCcpO1xuICB9XG5cbiAgLy8gdHNsaW50OmRpc2FibGUtbmV4dC1saW5lOm5vLWFueVxuICBpc0FycmF5T2ZOelRyZWVOb2RlKHZhbHVlOiBhbnlbXSk6IGJvb2xlYW4ge1xuICAgIHJldHVybiB2YWx1ZS5ldmVyeShpdGVtID0+IGl0ZW0gaW5zdGFuY2VvZiBOelRyZWVOb2RlKTtcbiAgfVxuXG4gIC8qKlxuICAgKiByZXNldCBzZWxlY3RlZE5vZGVMaXN0XG4gICAqL1xuICBjYWxjU2VsZWN0ZWRLZXlzKHNlbGVjdGVkS2V5czogc3RyaW5nW10sIG56Tm9kZXM6IE56VHJlZU5vZGVbXSwgaXNNdWx0aTogYm9vbGVhbiA9IGZhbHNlKTogdm9pZCB7XG4gICAgY29uc3QgY2FsYyA9IChub2RlczogTnpUcmVlTm9kZVtdKTogYm9vbGVhbiA9PiB7XG4gICAgICByZXR1cm4gbm9kZXMuZXZlcnkobm9kZSA9PiB7XG4gICAgICAgIGlmIChpc0luQXJyYXkobm9kZS5rZXksIHNlbGVjdGVkS2V5cykpIHtcbiAgICAgICAgICBub2RlLmlzU2VsZWN0ZWQgPSB0cnVlO1xuICAgICAgICAgIGlmICghaXNNdWx0aSkge1xuICAgICAgICAgICAgLy8gaWYgbm90IHN1cHBvcnQgbXVsdGkgc2VsZWN0XG4gICAgICAgICAgICByZXR1cm4gZmFsc2U7XG4gICAgICAgICAgfVxuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIG5vZGUuaXNTZWxlY3RlZCA9IGZhbHNlO1xuICAgICAgICB9XG4gICAgICAgIGlmIChub2RlLmNoaWxkcmVuLmxlbmd0aCA+IDApIHtcbiAgICAgICAgICAvLyBSZWN1cnNpb25cbiAgICAgICAgICByZXR1cm4gY2FsYyhub2RlLmNoaWxkcmVuKTtcbiAgICAgICAgfVxuICAgICAgICByZXR1cm4gdHJ1ZTtcbiAgICAgIH0pO1xuICAgIH07XG4gICAgY2FsYyhuek5vZGVzKTtcbiAgfVxuXG4gIC8qKlxuICAgKiByZXNldCBleHBhbmRlZE5vZGVMaXN0XG4gICAqL1xuICBjYWxjRXhwYW5kZWRLZXlzKGV4cGFuZGVkS2V5czogc3RyaW5nW10sIG56Tm9kZXM6IE56VHJlZU5vZGVbXSk6IHZvaWQge1xuICAgIHRoaXMuZXhwYW5kZWROb2RlTGlzdCA9IFtdO1xuICAgIGNvbnN0IGNhbGMgPSAobm9kZXM6IE56VHJlZU5vZGVbXSkgPT4ge1xuICAgICAgbm9kZXMuZm9yRWFjaChub2RlID0+IHtcbiAgICAgICAgaWYgKGlzSW5BcnJheShub2RlLmtleSwgZXhwYW5kZWRLZXlzKSkge1xuICAgICAgICAgIG5vZGUuaXNFeHBhbmRlZCA9IHRydWU7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgbm9kZS5pc0V4cGFuZGVkID0gZmFsc2U7XG4gICAgICAgIH1cbiAgICAgICAgaWYgKG5vZGUuY2hpbGRyZW4ubGVuZ3RoID4gMCkge1xuICAgICAgICAgIGNhbGMobm9kZS5jaGlsZHJlbik7XG4gICAgICAgIH1cbiAgICAgIH0pO1xuICAgIH07XG4gICAgY2FsYyhuek5vZGVzKTtcbiAgfVxuXG4gIC8qKlxuICAgKiByZXNldCBjaGVja2VkTm9kZUxpc3RcbiAgICovXG4gIGNhbGNDaGVja2VkS2V5cyhjaGVja2VkS2V5czogc3RyaW5nW10sIG56Tm9kZXM6IE56VHJlZU5vZGVbXSwgaXNDaGVja1N0cmljdGx5OiBib29sZWFuID0gZmFsc2UpOiB2b2lkIHtcbiAgICB0aGlzLmNoZWNrZWROb2RlTGlzdCA9IFtdO1xuICAgIHRoaXMuaGFsZkNoZWNrZWROb2RlTGlzdCA9IFtdO1xuICAgIGNvbnN0IGNhbGMgPSAobm9kZXM6IE56VHJlZU5vZGVbXSkgPT4ge1xuICAgICAgbm9kZXMuZm9yRWFjaChub2RlID0+IHtcbiAgICAgICAgaWYgKGlzSW5BcnJheShub2RlLmtleSwgY2hlY2tlZEtleXMpKSB7XG4gICAgICAgICAgbm9kZS5pc0NoZWNrZWQgPSB0cnVlO1xuICAgICAgICAgIG5vZGUuaXNIYWxmQ2hlY2tlZCA9IGZhbHNlO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIG5vZGUuaXNDaGVja2VkID0gZmFsc2U7XG4gICAgICAgICAgbm9kZS5pc0hhbGZDaGVja2VkID0gZmFsc2U7XG4gICAgICAgIH1cbiAgICAgICAgaWYgKG5vZGUuY2hpbGRyZW4ubGVuZ3RoID4gMCkge1xuICAgICAgICAgIGNhbGMobm9kZS5jaGlsZHJlbik7XG4gICAgICAgIH1cbiAgICAgIH0pO1xuICAgIH07XG4gICAgY2FsYyhuek5vZGVzKTtcbiAgICAvLyBjb250cm9sbGVkIHN0YXRlXG4gICAgdGhpcy5yZWZyZXNoQ2hlY2tTdGF0ZShpc0NoZWNrU3RyaWN0bHkpO1xuICB9XG5cbiAgLyoqXG4gICAqIHNldCBkcmFnIG5vZGVcbiAgICovXG4gIHNldFNlbGVjdGVkTm9kZShub2RlOiBOelRyZWVOb2RlKTogdm9pZCB7XG4gICAgdGhpcy5zZWxlY3RlZE5vZGUgPSBub2RlO1xuICB9XG5cbiAgLyoqXG4gICAqIHNldCBub2RlIHNlbGVjdGVkIHN0YXR1c1xuICAgKi9cbiAgc2V0Tm9kZUFjdGl2ZShub2RlOiBOelRyZWVOb2RlKTogdm9pZCB7XG4gICAgaWYgKCF0aGlzLmlzTXVsdGlwbGUgJiYgbm9kZS5pc1NlbGVjdGVkKSB7XG4gICAgICB0aGlzLnNlbGVjdGVkTm9kZUxpc3QuZm9yRWFjaChuID0+IHtcbiAgICAgICAgaWYgKG5vZGUua2V5ICE9PSBuLmtleSkge1xuICAgICAgICAgIC8vIHJlc2V0IG90aGVyIG5vZGVzXG4gICAgICAgICAgbi5pc1NlbGVjdGVkID0gZmFsc2U7XG4gICAgICAgIH1cbiAgICAgIH0pO1xuICAgICAgLy8gc2luZ2xlIG1vZGU6IHJlbW92ZSBwcmUgbm9kZVxuICAgICAgdGhpcy5zZWxlY3RlZE5vZGVMaXN0ID0gW107XG4gICAgfVxuICAgIHRoaXMuc2V0U2VsZWN0ZWROb2RlTGlzdChub2RlLCB0aGlzLmlzTXVsdGlwbGUpO1xuICB9XG5cbiAgLyoqXG4gICAqIGFkZCBvciByZW1vdmUgbm9kZSB0byBzZWxlY3RlZE5vZGVMaXN0XG4gICAqL1xuICBzZXRTZWxlY3RlZE5vZGVMaXN0KG5vZGU6IE56VHJlZU5vZGUsIGlzTXVsdGlwbGU6IGJvb2xlYW4gPSBmYWxzZSk6IHZvaWQge1xuICAgIGNvbnN0IGluZGV4ID0gdGhpcy5zZWxlY3RlZE5vZGVMaXN0LmZpbmRJbmRleChuID0+IG5vZGUua2V5ID09PSBuLmtleSk7XG4gICAgaWYgKGlzTXVsdGlwbGUpIHtcbiAgICAgIGlmIChub2RlLmlzU2VsZWN0ZWQgJiYgaW5kZXggPT09IC0xKSB7XG4gICAgICAgIHRoaXMuc2VsZWN0ZWROb2RlTGlzdC5wdXNoKG5vZGUpO1xuICAgICAgfVxuICAgIH0gZWxzZSB7XG4gICAgICBpZiAobm9kZS5pc1NlbGVjdGVkICYmIGluZGV4ID09PSAtMSkge1xuICAgICAgICB0aGlzLnNlbGVjdGVkTm9kZUxpc3QgPSBbIG5vZGUgXTtcbiAgICAgIH1cbiAgICB9XG4gICAgaWYgKCFub2RlLmlzU2VsZWN0ZWQpIHtcbiAgICAgIHRoaXMuc2VsZWN0ZWROb2RlTGlzdCA9IHRoaXMuc2VsZWN0ZWROb2RlTGlzdC5maWx0ZXIobiA9PiAobi5rZXkgIT09IG5vZGUua2V5KSk7XG4gICAgfVxuICB9XG5cbiAgLyoqXG4gICAqIG1lcmdlIGNoZWNrZWQgbm9kZXNcbiAgICovXG4gIHNldEhhbGZDaGVja2VkTm9kZUxpc3Qobm9kZTogTnpUcmVlTm9kZSk6IHZvaWQge1xuICAgIGNvbnN0IGluZGV4ID0gdGhpcy5oYWxmQ2hlY2tlZE5vZGVMaXN0LmZpbmRJbmRleChuID0+IG5vZGUua2V5ID09PSBuLmtleSk7XG4gICAgaWYgKG5vZGUuaXNIYWxmQ2hlY2tlZCAmJiBpbmRleCA9PT0gLTEpIHtcbiAgICAgIHRoaXMuaGFsZkNoZWNrZWROb2RlTGlzdC5wdXNoKG5vZGUpO1xuICAgIH0gZWxzZSBpZiAoIW5vZGUuaXNIYWxmQ2hlY2tlZCAmJiBpbmRleCA+IC0xKSB7XG4gICAgICB0aGlzLmhhbGZDaGVja2VkTm9kZUxpc3QgPSB0aGlzLmhhbGZDaGVja2VkTm9kZUxpc3QuZmlsdGVyKG4gPT4gbm9kZS5rZXkgIT09IG4ua2V5KTtcbiAgICB9XG4gIH1cblxuICBzZXRDaGVja2VkTm9kZUxpc3Qobm9kZTogTnpUcmVlTm9kZSk6IHZvaWQge1xuICAgIGNvbnN0IGluZGV4ID0gdGhpcy5jaGVja2VkTm9kZUxpc3QuZmluZEluZGV4KG4gPT4gbm9kZS5rZXkgPT09IG4ua2V5KTtcbiAgICBpZiAobm9kZS5pc0NoZWNrZWQgJiYgaW5kZXggPT09IC0xKSB7XG4gICAgICB0aGlzLmNoZWNrZWROb2RlTGlzdC5wdXNoKG5vZGUpO1xuICAgIH0gZWxzZSBpZiAoIW5vZGUuaXNDaGVja2VkICYmIGluZGV4ID4gLTEpIHtcbiAgICAgIHRoaXMuY2hlY2tlZE5vZGVMaXN0ID0gdGhpcy5jaGVja2VkTm9kZUxpc3QuZmlsdGVyKG4gPT4gbm9kZS5rZXkgIT09IG4ua2V5KTtcbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogY29uZHVjdCBjaGVja2VkL3NlbGVjdGVkL2V4cGFuZGVkIGtleXNcbiAgICovXG4gIGNvbmR1Y3ROb2RlU3RhdGUodHlwZTogc3RyaW5nID0gJ2NoZWNrJyk6IE56VHJlZU5vZGVbXSB7XG4gICAgbGV0IHJlc3VsdE5vZGVzTGlzdDogTnpUcmVlTm9kZVtdID0gW107XG4gICAgc3dpdGNoICh0eXBlKSB7XG4gICAgICBjYXNlICdzZWxlY3QnOlxuICAgICAgICByZXN1bHROb2Rlc0xpc3QgPSB0aGlzLnNlbGVjdGVkTm9kZUxpc3Q7XG4gICAgICAgIGJyZWFrO1xuICAgICAgY2FzZSAnZXhwYW5kJzpcbiAgICAgICAgcmVzdWx0Tm9kZXNMaXN0ID0gdGhpcy5leHBhbmRlZE5vZGVMaXN0O1xuICAgICAgICBicmVhaztcbiAgICAgIGNhc2UgJ21hdGNoJzpcbiAgICAgICAgcmVzdWx0Tm9kZXNMaXN0ID0gdGhpcy5tYXRjaGVkTm9kZUxpc3Q7XG4gICAgICAgIGJyZWFrO1xuICAgICAgY2FzZSAnY2hlY2snOlxuICAgICAgICByZXN1bHROb2Rlc0xpc3QgPSB0aGlzLmNoZWNrZWROb2RlTGlzdDtcbiAgICAgICAgY29uc3QgaXNJZ25vcmUgPSAobm9kZTogTnpUcmVlTm9kZSk6IGJvb2xlYW4gPT4ge1xuICAgICAgICAgIGNvbnN0IHBhcmVudE5vZGUgPSBub2RlLmdldFBhcmVudE5vZGUoKTtcbiAgICAgICAgICBpZiAocGFyZW50Tm9kZSkge1xuICAgICAgICAgICAgaWYgKHRoaXMuY2hlY2tlZE5vZGVMaXN0LmZpbmRJbmRleChuID0+IG4ua2V5ID09PSBwYXJlbnROb2RlLmtleSkgPiAtMSkge1xuICAgICAgICAgICAgICByZXR1cm4gdHJ1ZTtcbiAgICAgICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICAgIHJldHVybiBpc0lnbm9yZShwYXJlbnROb2RlKTtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICB9XG4gICAgICAgICAgcmV0dXJuIGZhbHNlO1xuICAgICAgICB9O1xuICAgICAgICAvLyBtZXJnZSBjaGVja2VkXG4gICAgICAgIGlmICghdGhpcy5pc0NoZWNrU3RyaWN0bHkpIHtcbiAgICAgICAgICByZXN1bHROb2Rlc0xpc3QgPSB0aGlzLmNoZWNrZWROb2RlTGlzdC5maWx0ZXIobiA9PiAhaXNJZ25vcmUobikpO1xuICAgICAgICB9XG4gICAgICAgIGJyZWFrO1xuICAgICAgY2FzZSAnaGFsZkNoZWNrJzpcbiAgICAgICAgaWYgKCF0aGlzLmlzQ2hlY2tTdHJpY3RseSkge1xuICAgICAgICAgIHJlc3VsdE5vZGVzTGlzdCA9IHRoaXMuaGFsZkNoZWNrZWROb2RlTGlzdDtcbiAgICAgICAgfVxuICAgICAgICBicmVhaztcbiAgICB9XG4gICAgcmV0dXJuIHJlc3VsdE5vZGVzTGlzdDtcbiAgfVxuXG4gIC8qKlxuICAgKiBzZXQgZXhwYW5kZWQgbm9kZXNcbiAgICovXG4gIHNldEV4cGFuZGVkTm9kZUxpc3Qobm9kZTogTnpUcmVlTm9kZSk6IHZvaWQge1xuICAgIGlmIChub2RlLmlzTGVhZikge1xuICAgICAgcmV0dXJuO1xuICAgIH1cbiAgICBjb25zdCBpbmRleCA9IHRoaXMuZXhwYW5kZWROb2RlTGlzdC5maW5kSW5kZXgobiA9PiBub2RlLmtleSA9PT0gbi5rZXkpO1xuICAgIGlmIChub2RlLmlzRXhwYW5kZWQgJiYgaW5kZXggPT09IC0xKSB7XG4gICAgICB0aGlzLmV4cGFuZGVkTm9kZUxpc3QucHVzaChub2RlKTtcbiAgICB9IGVsc2UgaWYgKCFub2RlLmlzRXhwYW5kZWQgJiYgaW5kZXggPiAtMSkge1xuICAgICAgdGhpcy5leHBhbmRlZE5vZGVMaXN0ID0gdGhpcy5leHBhbmRlZE5vZGVMaXN0LmZpbHRlcihuID0+IG5vZGUua2V5ICE9PSBuLmtleSk7XG4gICAgfVxuICB9XG5cbiAgLyoqXG4gICAqIGNoZWNrIHN0YXRlXG4gICAqIEBwYXJhbSBub2RlXG4gICAqL1xuICByZWZyZXNoQ2hlY2tTdGF0ZShpc0NoZWNrU3RyaWN0bHk6IGJvb2xlYW4gPSBmYWxzZSk6IHZvaWQge1xuICAgIGlmIChpc0NoZWNrU3RyaWN0bHkpIHtcbiAgICAgIHJldHVybjtcbiAgICB9XG4gICAgdGhpcy5jaGVja2VkTm9kZUxpc3QuZm9yRWFjaChub2RlID0+IHtcbiAgICAgIHRoaXMuY29uZHVjdChub2RlKTtcbiAgICB9KTtcbiAgfVxuXG4gIC8vIHJlc2V0IG90aGVyIG5vZGUgY2hlY2tlZCBzdGF0ZSBiYXNlZCBjdXJyZW50IG5vZGVcbiAgY29uZHVjdChub2RlOiBOelRyZWVOb2RlKTogdm9pZCB7XG4gICAgY29uc3QgaXNDaGVja2VkID0gbm9kZS5pc0NoZWNrZWQ7XG4gICAgaWYgKG5vZGUpIHtcbiAgICAgIHRoaXMuY29uZHVjdFVwKG5vZGUpO1xuICAgICAgdGhpcy5jb25kdWN0RG93bihub2RlLCBpc0NoZWNrZWQpO1xuICAgIH1cbiAgfVxuXG4gIC8qKlxuICAgKiAx44CBY2hpbGRyZW4gaGFsZiBjaGVja2VkXG4gICAqIDLjgIFjaGlsZHJlbiBhbGwgY2hlY2tlZCwgcGFyZW50IGNoZWNrZWRcbiAgICogM+OAgW5vIGNoaWxkcmVuIGNoZWNrZWRcbiAgICovXG4gIGNvbmR1Y3RVcChub2RlOiBOelRyZWVOb2RlKTogdm9pZCB7XG4gICAgY29uc3QgcGFyZW50Tm9kZSA9IG5vZGUuZ2V0UGFyZW50Tm9kZSgpO1xuICAgIC8vIOWFqOemgeeUqOiKgueCueS4jemAieS4rVxuICAgIGlmIChwYXJlbnROb2RlKSB7XG4gICAgICBpZiAoIWlzQ2hlY2tEaXNhYmxlZChwYXJlbnROb2RlKSkge1xuICAgICAgICBpZiAocGFyZW50Tm9kZS5jaGlsZHJlbi5ldmVyeShjaGlsZCA9PiBpc0NoZWNrRGlzYWJsZWQoY2hpbGQpIHx8ICghY2hpbGQuaXNIYWxmQ2hlY2tlZCAmJiBjaGlsZC5pc0NoZWNrZWQpKSkge1xuICAgICAgICAgIHBhcmVudE5vZGUuaXNDaGVja2VkID0gdHJ1ZTtcbiAgICAgICAgICBwYXJlbnROb2RlLmlzSGFsZkNoZWNrZWQgPSBmYWxzZTtcbiAgICAgICAgfSBlbHNlIGlmIChwYXJlbnROb2RlLmNoaWxkcmVuLnNvbWUoY2hpbGQgPT4gY2hpbGQuaXNIYWxmQ2hlY2tlZCB8fCBjaGlsZC5pc0NoZWNrZWQpKSB7XG4gICAgICAgICAgcGFyZW50Tm9kZS5pc0NoZWNrZWQgPSBmYWxzZTtcbiAgICAgICAgICBwYXJlbnROb2RlLmlzSGFsZkNoZWNrZWQgPSB0cnVlO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIHBhcmVudE5vZGUuaXNDaGVja2VkID0gZmFsc2U7XG4gICAgICAgICAgcGFyZW50Tm9kZS5pc0hhbGZDaGVja2VkID0gZmFsc2U7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICAgIHRoaXMuc2V0Q2hlY2tlZE5vZGVMaXN0KHBhcmVudE5vZGUpO1xuICAgICAgdGhpcy5zZXRIYWxmQ2hlY2tlZE5vZGVMaXN0KHBhcmVudE5vZGUpO1xuICAgICAgdGhpcy5jb25kdWN0VXAocGFyZW50Tm9kZSk7XG4gICAgfVxuICB9XG5cbiAgLyoqXG4gICAqIHJlc2V0IGNoaWxkIGNoZWNrIHN0YXRlXG4gICAqL1xuICBjb25kdWN0RG93bihub2RlOiBOelRyZWVOb2RlLCB2YWx1ZTogYm9vbGVhbik6IHZvaWQge1xuICAgIGlmICghaXNDaGVja0Rpc2FibGVkKG5vZGUpKSB7XG4gICAgICBub2RlLmlzQ2hlY2tlZCA9IHZhbHVlO1xuICAgICAgbm9kZS5pc0hhbGZDaGVja2VkID0gZmFsc2U7XG4gICAgICB0aGlzLnNldENoZWNrZWROb2RlTGlzdChub2RlKTtcbiAgICAgIHRoaXMuc2V0SGFsZkNoZWNrZWROb2RlTGlzdChub2RlKTtcbiAgICAgIG5vZGUuY2hpbGRyZW4uZm9yRWFjaChuID0+IHtcbiAgICAgICAgdGhpcy5jb25kdWN0RG93bihuLCB2YWx1ZSk7XG4gICAgICB9KTtcbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogc2VhcmNoIHZhbHVlICYgZXhwYW5kIG5vZGVcbiAgICogc2hvdWxkIGFkZCBleHBhbmRsaXN0XG4gICAqL1xuICBzZWFyY2hFeHBhbmQodmFsdWU6IHN0cmluZyk6IHZvaWQge1xuICAgIHRoaXMubWF0Y2hlZE5vZGVMaXN0ID0gW107XG4gICAgY29uc3QgZXhwYW5kZWRLZXlzOiBzdHJpbmdbXSA9IFtdO1xuICAgIGlmICghaXNOb3ROaWwodmFsdWUpKSB7XG4gICAgICByZXR1cm47XG4gICAgfVxuICAgIC8vIHRvIHJlc2V0IGV4cGFuZGVkTm9kZUxpc3RcbiAgICBjb25zdCBleHBhbmRQYXJlbnQgPSAobjogTnpUcmVlTm9kZSkgPT4ge1xuICAgICAgLy8gZXhwYW5kIHBhcmVudCBub2RlXG4gICAgICBjb25zdCBwYXJlbnROb2RlID0gbi5nZXRQYXJlbnROb2RlKCk7XG4gICAgICBpZiAocGFyZW50Tm9kZSkge1xuICAgICAgICBleHBhbmRlZEtleXMucHVzaChwYXJlbnROb2RlLmtleSk7XG4gICAgICAgIGV4cGFuZFBhcmVudChwYXJlbnROb2RlKTtcbiAgICAgIH1cbiAgICB9O1xuICAgIGNvbnN0IHNlYXJjaENoaWxkID0gKG46IE56VHJlZU5vZGUpID0+IHtcbiAgICAgIGlmICh2YWx1ZSAmJiBuLnRpdGxlLmluY2x1ZGVzKHZhbHVlKSkge1xuICAgICAgICAvLyBtYXRjaCB0aGUgbm9kZVxuICAgICAgICBuLmlzTWF0Y2hlZCA9IHRydWU7XG4gICAgICAgIHRoaXMubWF0Y2hlZE5vZGVMaXN0LnB1c2gobik7XG4gICAgICAgIC8vIGV4cGFuZCBwYXJlbnROb2RlXG4gICAgICAgIGV4cGFuZFBhcmVudChuKTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIG4uaXNNYXRjaGVkID0gZmFsc2U7XG4gICAgICB9XG4gICAgICBuLmNoaWxkcmVuLmZvckVhY2goY2hpbGQgPT4ge1xuICAgICAgICBzZWFyY2hDaGlsZChjaGlsZCk7XG4gICAgICB9KTtcbiAgICB9O1xuICAgIHRoaXMucm9vdE5vZGVzLmZvckVhY2goY2hpbGQgPT4ge1xuICAgICAgc2VhcmNoQ2hpbGQoY2hpbGQpO1xuICAgIH0pO1xuICAgIC8vIGV4cGFuZCBtYXRjaGVkIGtleXNcbiAgICB0aGlzLmNhbGNFeHBhbmRlZEtleXMoZXhwYW5kZWRLZXlzLCB0aGlzLnJvb3ROb2Rlcyk7XG4gIH1cblxuICAvKipcbiAgICogZmx1c2ggYWZ0ZXIgZGVsZXRlIG5vZGVcbiAgICovXG4gIGFmdGVyUmVtb3ZlKG5vZGVzOiBOelRyZWVOb2RlW10pOiB2b2lkIHtcbiAgICAvLyB0byByZXNldCBzZWxlY3RlZE5vZGVMaXN0ICYgZXhwYW5kZWROb2RlTGlzdFxuICAgIGNvbnN0IGxvb3BOb2RlID0gKG5vZGU6IE56VHJlZU5vZGUpID0+IHtcbiAgICAgIC8vIHJlbW92ZSBzZWxlY3RlZCBub2RlXG4gICAgICB0aGlzLnNlbGVjdGVkTm9kZUxpc3QgPSB0aGlzLnNlbGVjdGVkTm9kZUxpc3QuZmlsdGVyKG4gPT4gbi5rZXkgIT09IG5vZGUua2V5KTtcbiAgICAgIC8vIHJlbW92ZSBleHBhbmRlZCBub2RlXG4gICAgICB0aGlzLmV4cGFuZGVkTm9kZUxpc3QgPSB0aGlzLmV4cGFuZGVkTm9kZUxpc3QuZmlsdGVyKG4gPT4gbi5rZXkgIT09IG5vZGUua2V5KTtcbiAgICAgIC8vIHJlbW92ZSBjaGVja2VkIG5vZGVcbiAgICAgIHRoaXMuY2hlY2tlZE5vZGVMaXN0ID0gdGhpcy5jaGVja2VkTm9kZUxpc3QuZmlsdGVyKG4gPT4gbi5rZXkgIT09IG5vZGUua2V5KTtcbiAgICAgIGlmIChub2RlLmNoaWxkcmVuKSB7XG4gICAgICAgIG5vZGUuY2hpbGRyZW4uZm9yRWFjaChjaGlsZCA9PiB7XG4gICAgICAgICAgbG9vcE5vZGUoY2hpbGQpO1xuICAgICAgICB9KTtcbiAgICAgIH1cbiAgICB9O1xuICAgIG5vZGVzLmZvckVhY2gobiA9PiB7XG4gICAgICBsb29wTm9kZShuKTtcbiAgICB9KTtcbiAgICB0aGlzLnJlZnJlc2hDaGVja1N0YXRlKHRoaXMuaXNDaGVja1N0cmljdGx5KTtcbiAgfVxuXG4gIC8qKlxuICAgKiBkcmFnIGV2ZW50XG4gICAqL1xuICByZWZyZXNoRHJhZ05vZGUobm9kZTogTnpUcmVlTm9kZSk6IHZvaWQge1xuICAgIGlmIChub2RlLmNoaWxkcmVuLmxlbmd0aCA9PT0gMCkge1xuICAgICAgLy8gdW50aWwgcm9vdFxuICAgICAgdGhpcy5jb25kdWN0VXAobm9kZSk7XG4gICAgfSBlbHNlIHtcbiAgICAgIG5vZGUuY2hpbGRyZW4uZm9yRWFjaCgoY2hpbGQpID0+IHtcbiAgICAgICAgdGhpcy5yZWZyZXNoRHJhZ05vZGUoY2hpbGQpO1xuICAgICAgfSk7XG4gICAgfVxuICB9XG5cbiAgLy8gcmVzZXQgbm9kZSBsZXZlbFxuICByZXNldE5vZGVMZXZlbChub2RlOiBOelRyZWVOb2RlKTogdm9pZCB7XG4gICAgY29uc3QgcGFyZW50Tm9kZSA9IG5vZGUuZ2V0UGFyZW50Tm9kZSgpO1xuICAgIGlmIChwYXJlbnROb2RlKSB7XG4gICAgICBub2RlLmxldmVsID0gcGFyZW50Tm9kZS5sZXZlbCArIDE7XG4gICAgfSBlbHNlIHtcbiAgICAgIG5vZGUubGV2ZWwgPSAwO1xuICAgIH1cbiAgICBmb3IgKGNvbnN0IGNoaWxkIG9mIG5vZGUuY2hpbGRyZW4pIHtcbiAgICAgIHRoaXMucmVzZXROb2RlTGV2ZWwoY2hpbGQpO1xuICAgIH1cbiAgfVxuXG4gIGNhbGNEcm9wUG9zaXRpb24oZXZlbnQ6IERyYWdFdmVudCk6IG51bWJlciB7XG4gICAgY29uc3QgeyBjbGllbnRZIH0gPSBldmVudDtcbiAgICAvLyB0byBmaXggZmlyZWZveCB1bmRlZmluZWRcbiAgICBjb25zdCB7IHRvcCwgYm90dG9tLCBoZWlnaHQgfSA9IGV2ZW50LnNyY0VsZW1lbnQgPyBldmVudC5zcmNFbGVtZW50LmdldEJvdW5kaW5nQ2xpZW50UmVjdCgpIDogKGV2ZW50LnRhcmdldCBhcyBFbGVtZW50KS5nZXRCb3VuZGluZ0NsaWVudFJlY3QoKTtcbiAgICBjb25zdCBkZXMgPSBNYXRoLm1heChoZWlnaHQgKiB0aGlzLkRSQUdfU0lERV9SQU5HRSwgdGhpcy5EUkFHX01JTl9HQVApO1xuXG4gICAgaWYgKGNsaWVudFkgPD0gdG9wICsgZGVzKSB7XG4gICAgICByZXR1cm4gLTE7XG4gICAgfSBlbHNlIGlmIChjbGllbnRZID49IGJvdHRvbSAtIGRlcykge1xuICAgICAgcmV0dXJuIDE7XG4gICAgfVxuXG4gICAgcmV0dXJuIDA7XG4gIH1cblxuICAvKipcbiAgICogZHJvcFxuICAgKiAwOiBpbm5lciAtMTogcHJlIDE6IG5leHRcbiAgICovXG4gIGRyb3BBbmRBcHBseSh0YXJnZXROb2RlOiBOelRyZWVOb2RlLCBkcmFnUG9zOiBudW1iZXIgPSAtMSk6IHZvaWQge1xuICAgIGlmICghdGFyZ2V0Tm9kZSB8fCBkcmFnUG9zID4gMSkge1xuICAgICAgcmV0dXJuO1xuICAgIH1cbiAgICBjb25zdCB0cmVlU2VydmljZSA9IHRhcmdldE5vZGUudHJlZVNlcnZpY2U7XG4gICAgY29uc3QgdGFyZ2V0UGFyZW50ID0gdGFyZ2V0Tm9kZS5nZXRQYXJlbnROb2RlKCk7XG4gICAgY29uc3QgaXNTZWxlY3RlZFJvb3ROb2RlID0gdGhpcy5zZWxlY3RlZE5vZGUuZ2V0UGFyZW50Tm9kZSgpO1xuICAgIC8vIHJlbW92ZSB0aGUgZHJhZ05vZGVcbiAgICBpZiAoaXNTZWxlY3RlZFJvb3ROb2RlKSB7XG4gICAgICBpc1NlbGVjdGVkUm9vdE5vZGUuY2hpbGRyZW4gPSBpc1NlbGVjdGVkUm9vdE5vZGUuY2hpbGRyZW4uZmlsdGVyKG4gPT4gbi5rZXkgIT09IHRoaXMuc2VsZWN0ZWROb2RlLmtleSk7XG4gICAgfSBlbHNlIHtcbiAgICAgIHRoaXMucm9vdE5vZGVzID0gdGhpcy5yb290Tm9kZXMuZmlsdGVyKG4gPT4gbi5rZXkgIT09IHRoaXMuc2VsZWN0ZWROb2RlLmtleSk7XG4gICAgfVxuICAgIHN3aXRjaCAoZHJhZ1Bvcykge1xuICAgICAgY2FzZSAwOlxuICAgICAgICB0YXJnZXROb2RlLmFkZENoaWxkcmVuKFsgdGhpcy5zZWxlY3RlZE5vZGUgXSk7XG4gICAgICAgIHRoaXMucmVzZXROb2RlTGV2ZWwodGFyZ2V0Tm9kZSk7XG4gICAgICAgIGJyZWFrO1xuICAgICAgY2FzZSAtMTpcbiAgICAgIGNhc2UgMTpcbiAgICAgICAgY29uc3QgdEluZGV4ID0gZHJhZ1BvcyA9PT0gMSA/IDEgOiAwO1xuICAgICAgICBpZiAodGFyZ2V0UGFyZW50KSB7XG4gICAgICAgICAgdGFyZ2V0UGFyZW50LmFkZENoaWxkcmVuKFsgdGhpcy5zZWxlY3RlZE5vZGUgXSwgdGFyZ2V0UGFyZW50LmNoaWxkcmVuLmluZGV4T2YodGFyZ2V0Tm9kZSkgKyB0SW5kZXgpO1xuICAgICAgICAgIGNvbnN0IHBhcmVudE5vZGUgPSB0aGlzLnNlbGVjdGVkTm9kZS5nZXRQYXJlbnROb2RlKCk7XG4gICAgICAgICAgaWYgKHBhcmVudE5vZGUpIHtcbiAgICAgICAgICAgIHRoaXMucmVzZXROb2RlTGV2ZWwocGFyZW50Tm9kZSk7XG4gICAgICAgICAgfVxuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIGNvbnN0IHRhcmdldEluZGV4ID0gdGhpcy5yb290Tm9kZXMuaW5kZXhPZih0YXJnZXROb2RlKSArIHRJbmRleDtcbiAgICAgICAgICAvLyDmoLnoioLngrnmj5LlhaVcbiAgICAgICAgICB0aGlzLnJvb3ROb2Rlcy5zcGxpY2UodGFyZ2V0SW5kZXgsIDAsIHRoaXMuc2VsZWN0ZWROb2RlKTtcbiAgICAgICAgICB0aGlzLnJvb3ROb2Rlc1sgdGFyZ2V0SW5kZXggXS5wYXJlbnROb2RlID0gbnVsbDtcbiAgICAgICAgICB0aGlzLnJvb3ROb2Rlc1sgdGFyZ2V0SW5kZXggXS5sZXZlbCA9IDA7XG4gICAgICAgIH1cbiAgICAgICAgYnJlYWs7XG4gICAgfVxuICAgIC8vIGZsdXNoIGFsbCBub2Rlc1xuICAgIHRoaXMucm9vdE5vZGVzLmZvckVhY2goKGNoaWxkKSA9PiB7XG4gICAgICBpZiAoIWNoaWxkLnRyZWVTZXJ2aWNlKSB7XG4gICAgICAgIGNoaWxkLnNlcnZpY2UgPSB0cmVlU2VydmljZTtcbiAgICAgIH1cbiAgICAgIHRoaXMucmVmcmVzaERyYWdOb2RlKGNoaWxkKTtcbiAgICB9KTtcbiAgfVxuXG4gIC8qKlxuICAgKiBlbWl0IFN0cnVjdHVyZVxuICAgKiBldmVudE5hbWVcbiAgICogbm9kZVxuICAgKiBldmVudDogTW91c2VFdmVudCAvIERyYWdFdmVudFxuICAgKiBkcmFnTm9kZVxuICAgKi9cbiAgZm9ybWF0RXZlbnQoZXZlbnROYW1lOiBzdHJpbmcsIG5vZGU6IE56VHJlZU5vZGUgfCBudWxsLCBldmVudDogTW91c2VFdmVudCB8IERyYWdFdmVudCB8IG51bGwpOiBOekZvcm1hdEVtaXRFdmVudCB7XG4gICAgY29uc3QgZW1pdFN0cnVjdHVyZTogTnpGb3JtYXRFbWl0RXZlbnQgPSB7XG4gICAgICAnZXZlbnROYW1lJzogZXZlbnROYW1lLFxuICAgICAgJ25vZGUnICAgICA6IG5vZGUsXG4gICAgICAnZXZlbnQnICAgIDogZXZlbnRcbiAgICB9O1xuICAgIHN3aXRjaCAoZXZlbnROYW1lKSB7XG4gICAgICBjYXNlICdkcmFnc3RhcnQnOlxuICAgICAgY2FzZSAnZHJhZ2VudGVyJzpcbiAgICAgIGNhc2UgJ2RyYWdvdmVyJzpcbiAgICAgIGNhc2UgJ2RyYWdsZWF2ZSc6XG4gICAgICBjYXNlICdkcm9wJzpcbiAgICAgIGNhc2UgJ2RyYWdlbmQnOlxuICAgICAgICBPYmplY3QuYXNzaWduKGVtaXRTdHJ1Y3R1cmUsIHsgJ2RyYWdOb2RlJzogdGhpcy5nZXRTZWxlY3RlZE5vZGUoKSB9KTtcbiAgICAgICAgYnJlYWs7XG4gICAgICBjYXNlICdjbGljayc6XG4gICAgICBjYXNlICdkYmxjbGljayc6XG4gICAgICAgIE9iamVjdC5hc3NpZ24oZW1pdFN0cnVjdHVyZSwgeyAnc2VsZWN0ZWRLZXlzJzogdGhpcy5zZWxlY3RlZE5vZGVMaXN0IH0pO1xuICAgICAgICBPYmplY3QuYXNzaWduKGVtaXRTdHJ1Y3R1cmUsIHsgJ25vZGVzJzogdGhpcy5zZWxlY3RlZE5vZGVMaXN0IH0pO1xuICAgICAgICBPYmplY3QuYXNzaWduKGVtaXRTdHJ1Y3R1cmUsIHsgJ2tleXMnOiB0aGlzLnNlbGVjdGVkTm9kZUxpc3QubWFwKG4gPT4gbi5rZXkpIH0pO1xuICAgICAgICBicmVhaztcbiAgICAgIGNhc2UgJ2NoZWNrJzpcbiAgICAgICAgY29uc3QgY2hlY2tlZE5vZGVMaXN0ID0gdGhpcy5nZXRDaGVja2VkTm9kZUxpc3QoKTtcblxuICAgICAgICBPYmplY3QuYXNzaWduKGVtaXRTdHJ1Y3R1cmUsIHsgJ2NoZWNrZWRLZXlzJzogY2hlY2tlZE5vZGVMaXN0IH0pO1xuICAgICAgICBPYmplY3QuYXNzaWduKGVtaXRTdHJ1Y3R1cmUsIHsgJ25vZGVzJzogY2hlY2tlZE5vZGVMaXN0IH0pO1xuICAgICAgICBPYmplY3QuYXNzaWduKGVtaXRTdHJ1Y3R1cmUsIHsgJ2tleXMnOiBjaGVja2VkTm9kZUxpc3QubWFwKG4gPT4gbi5rZXkpIH0pO1xuICAgICAgICBicmVhaztcbiAgICAgIGNhc2UgJ3NlYXJjaCc6XG4gICAgICAgIE9iamVjdC5hc3NpZ24oZW1pdFN0cnVjdHVyZSwgeyAnbWF0Y2hlZEtleXMnOiB0aGlzLmdldE1hdGNoZWROb2RlTGlzdCgpIH0pO1xuICAgICAgICBPYmplY3QuYXNzaWduKGVtaXRTdHJ1Y3R1cmUsIHsgJ25vZGVzJzogdGhpcy5nZXRNYXRjaGVkTm9kZUxpc3QoKSB9KTtcbiAgICAgICAgT2JqZWN0LmFzc2lnbihlbWl0U3RydWN0dXJlLCB7ICdrZXlzJzogdGhpcy5nZXRNYXRjaGVkTm9kZUxpc3QoKS5tYXAobiA9PiBuLmtleSkgfSk7XG4gICAgICAgIGJyZWFrO1xuICAgICAgY2FzZSAnZXhwYW5kJzpcbiAgICAgICAgT2JqZWN0LmFzc2lnbihlbWl0U3RydWN0dXJlLCB7ICdub2Rlcyc6IHRoaXMuZXhwYW5kZWROb2RlTGlzdCB9KTtcbiAgICAgICAgT2JqZWN0LmFzc2lnbihlbWl0U3RydWN0dXJlLCB7ICdrZXlzJzogdGhpcy5leHBhbmRlZE5vZGVMaXN0Lm1hcChuID0+IG4ua2V5KSB9KTtcbiAgICAgICAgYnJlYWs7XG4gICAgfVxuICAgIHJldHVybiBlbWl0U3RydWN0dXJlO1xuICB9XG5cbiAgbmdPbkRlc3Ryb3koKTogdm9pZCB7XG4gICAgdGhpcy50cmlnZ2VyRXZlbnRDaGFuZ2UkLmNvbXBsZXRlKCk7XG4gIH1cbn1cbiJdfQ==