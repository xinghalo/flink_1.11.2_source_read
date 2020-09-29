/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * @record
 */
export function NzTreeNodeOptions() { }
if (false) {
    /** @type {?} */
    NzTreeNodeOptions.prototype.title;
    /** @type {?} */
    NzTreeNodeOptions.prototype.key;
    /** @type {?|undefined} */
    NzTreeNodeOptions.prototype.icon;
    /** @type {?|undefined} */
    NzTreeNodeOptions.prototype.isLeaf;
    /** @type {?|undefined} */
    NzTreeNodeOptions.prototype.checked;
    /** @type {?|undefined} */
    NzTreeNodeOptions.prototype.selected;
    /** @type {?|undefined} */
    NzTreeNodeOptions.prototype.selectable;
    /** @type {?|undefined} */
    NzTreeNodeOptions.prototype.disabled;
    /** @type {?|undefined} */
    NzTreeNodeOptions.prototype.disableCheckbox;
    /** @type {?|undefined} */
    NzTreeNodeOptions.prototype.expanded;
    /** @type {?|undefined} */
    NzTreeNodeOptions.prototype.children;
    /* Skipping unhandled member: [key: string]: any;*/
}
var NzTreeNode = /** @class */ (function () {
    function NzTreeNode(option, parent, service) {
        if (parent === void 0) { parent = null; }
        if (service === void 0) { service = null; }
        var _this = this;
        this.level = 0;
        if (option instanceof NzTreeNode) {
            return option;
        }
        this.service = service || null;
        this.origin = option;
        this.key = option.key || '';
        this.parentNode = parent;
        this._title = option.title || '---';
        this._icon = option.icon || '';
        this._isLeaf = option.isLeaf || false;
        this._children = [];
        // option params
        this._isChecked = option.checked || false;
        this._isSelectable = option.disabled || (option.selectable === false ? false : true);
        this._isDisabled = option.disabled || false;
        this._isDisableCheckbox = option.disableCheckbox || false;
        this._isExpanded = option.isLeaf ? false : option.expanded || false;
        this._isHalfChecked = false;
        this._isSelected = (!option.disabled && option.selected) || false;
        this._isLoading = false;
        this.isMatched = false;
        /**
         * parent's checked status will affect children while initializing
         */
        if (parent) {
            this.level = parent.level + 1;
        }
        else {
            this.level = 0;
        }
        if (typeof option.children !== 'undefined' && option.children !== null) {
            option.children.forEach((/**
             * @param {?} nodeOptions
             * @return {?}
             */
            function (nodeOptions) {
                /** @type {?} */
                var s = _this.treeService;
                if (s &&
                    !s.isCheckStrictly &&
                    option.checked &&
                    !option.disabled &&
                    !nodeOptions.disabled &&
                    !nodeOptions.disableCheckbox) {
                    nodeOptions.checked = option.checked;
                }
                _this._children.push(new NzTreeNode(nodeOptions, _this));
            }));
        }
    }
    Object.defineProperty(NzTreeNode.prototype, "treeService", {
        get: /**
         * @return {?}
         */
        function () {
            return this.service || (this.parentNode && this.parentNode.treeService);
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzTreeNode.prototype, "title", {
        /**
         * auto generate
         * get
         * set
         */
        get: /**
         * auto generate
         * get
         * set
         * @return {?}
         */
        function () {
            return this._title;
        },
        set: /**
         * @param {?} value
         * @return {?}
         */
        function (value) {
            this._title = value;
            this.update();
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzTreeNode.prototype, "icon", {
        get: /**
         * @return {?}
         */
        function () {
            return this._icon;
        },
        set: /**
         * @param {?} value
         * @return {?}
         */
        function (value) {
            this._icon = value;
            this.update();
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzTreeNode.prototype, "children", {
        get: /**
         * @return {?}
         */
        function () {
            return this._children;
        },
        set: /**
         * @param {?} value
         * @return {?}
         */
        function (value) {
            this._children = value;
            this.update();
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzTreeNode.prototype, "isLeaf", {
        get: /**
         * @return {?}
         */
        function () {
            return this._isLeaf;
        },
        set: /**
         * @param {?} value
         * @return {?}
         */
        function (value) {
            this._isLeaf = value;
            // this.update();
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzTreeNode.prototype, "isChecked", {
        get: /**
         * @return {?}
         */
        function () {
            return this._isChecked;
        },
        set: /**
         * @param {?} value
         * @return {?}
         */
        function (value) {
            this._isChecked = value;
            this._isAllChecked = value;
            this.origin.checked = value;
            this.afterValueChange('isChecked');
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzTreeNode.prototype, "isAllChecked", {
        get: /**
         * @return {?}
         */
        function () {
            return this._isAllChecked;
        },
        /**
         * @deprecated Maybe removed in next major version, use isChecked instead
         */
        set: /**
         * @deprecated Maybe removed in next major version, use isChecked instead
         * @param {?} value
         * @return {?}
         */
        function (value) {
            this._isAllChecked = value;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzTreeNode.prototype, "isHalfChecked", {
        get: /**
         * @return {?}
         */
        function () {
            return this._isHalfChecked;
        },
        set: /**
         * @param {?} value
         * @return {?}
         */
        function (value) {
            this._isHalfChecked = value;
            this.afterValueChange('isHalfChecked');
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzTreeNode.prototype, "isSelectable", {
        get: /**
         * @return {?}
         */
        function () {
            return this._isSelectable;
        },
        set: /**
         * @param {?} value
         * @return {?}
         */
        function (value) {
            this._isSelectable = value;
            this.update();
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzTreeNode.prototype, "isDisabled", {
        get: /**
         * @return {?}
         */
        function () {
            return this._isDisabled;
        },
        set: /**
         * @param {?} value
         * @return {?}
         */
        function (value) {
            this._isDisabled = value;
            this.update();
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzTreeNode.prototype, "isDisableCheckbox", {
        get: /**
         * @return {?}
         */
        function () {
            return this._isDisableCheckbox;
        },
        set: /**
         * @param {?} value
         * @return {?}
         */
        function (value) {
            this._isDisableCheckbox = value;
            this.update();
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzTreeNode.prototype, "isExpanded", {
        get: /**
         * @return {?}
         */
        function () {
            return this._isExpanded;
        },
        set: /**
         * @param {?} value
         * @return {?}
         */
        function (value) {
            this._isExpanded = value;
            this.origin.expanded = value;
            this.afterValueChange('isExpanded');
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzTreeNode.prototype, "isSelected", {
        get: /**
         * @return {?}
         */
        function () {
            return this._isSelected;
        },
        set: /**
         * @param {?} value
         * @return {?}
         */
        function (value) {
            this._isSelected = value;
            this.origin.selected = value;
            this.afterValueChange('isSelected');
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzTreeNode.prototype, "isLoading", {
        get: /**
         * @return {?}
         */
        function () {
            return this._isLoading;
        },
        set: /**
         * @param {?} value
         * @return {?}
         */
        function (value) {
            this._isLoading = value;
            this.update();
        },
        enumerable: true,
        configurable: true
    });
    /**
     * @param {?=} checked
     * @param {?=} halfChecked
     * @return {?}
     */
    NzTreeNode.prototype.setSyncChecked = /**
     * @param {?=} checked
     * @param {?=} halfChecked
     * @return {?}
     */
    function (checked, halfChecked) {
        if (checked === void 0) { checked = false; }
        if (halfChecked === void 0) { halfChecked = false; }
        this.setChecked(checked, halfChecked);
        if (this.treeService && !this.treeService.isCheckStrictly) {
            this.treeService.conduct(this);
        }
    };
    /**
     * @deprecated Maybe removed in next major version, use isChecked instead
     */
    /**
     * @deprecated Maybe removed in next major version, use isChecked instead
     * @param {?=} checked
     * @param {?=} halfChecked
     * @return {?}
     */
    NzTreeNode.prototype.setChecked = /**
     * @deprecated Maybe removed in next major version, use isChecked instead
     * @param {?=} checked
     * @param {?=} halfChecked
     * @return {?}
     */
    function (checked, halfChecked) {
        if (checked === void 0) { checked = false; }
        if (halfChecked === void 0) { halfChecked = false; }
        this.origin.checked = checked;
        this.isChecked = checked;
        this.isAllChecked = checked;
        this.isHalfChecked = halfChecked;
    };
    /**
     * @deprecated Maybe removed in next major version, use isExpanded instead
     */
    /**
     * @deprecated Maybe removed in next major version, use isExpanded instead
     * @param {?} value
     * @return {?}
     */
    NzTreeNode.prototype.setExpanded = /**
     * @deprecated Maybe removed in next major version, use isExpanded instead
     * @param {?} value
     * @return {?}
     */
    function (value) {
        this.isExpanded = value;
    };
    /**
     * @deprecated Maybe removed in next major version, use isSelected instead
     */
    /**
     * @deprecated Maybe removed in next major version, use isSelected instead
     * @param {?} value
     * @return {?}
     */
    NzTreeNode.prototype.setSelected = /**
     * @deprecated Maybe removed in next major version, use isSelected instead
     * @param {?} value
     * @return {?}
     */
    function (value) {
        if (this.isDisabled) {
            return;
        }
        this.isSelected = value;
    };
    /**
     * @return {?}
     */
    NzTreeNode.prototype.getParentNode = /**
     * @return {?}
     */
    function () {
        return this.parentNode;
    };
    /**
     * @return {?}
     */
    NzTreeNode.prototype.getChildren = /**
     * @return {?}
     */
    function () {
        return this.children;
    };
    /**
     * 支持按索引位置插入,叶子节点不可添加
     */
    // tslint:disable-next-line:no-any
    /**
     * 支持按索引位置插入,叶子节点不可添加
     * @param {?} children
     * @param {?=} childPos
     * @return {?}
     */
    // tslint:disable-next-line:no-any
    NzTreeNode.prototype.addChildren = /**
     * 支持按索引位置插入,叶子节点不可添加
     * @param {?} children
     * @param {?=} childPos
     * @return {?}
     */
    // tslint:disable-next-line:no-any
    function (children, childPos) {
        var _this = this;
        if (childPos === void 0) { childPos = -1; }
        if (!this.isLeaf) {
            children.forEach((/**
             * @param {?} node
             * @return {?}
             */
            function (node) {
                /** @type {?} */
                var refreshLevel = (/**
                 * @param {?} n
                 * @return {?}
                 */
                function (n) {
                    n.getChildren().forEach((/**
                     * @param {?} c
                     * @return {?}
                     */
                    function (c) {
                        c.level = (/** @type {?} */ (c.getParentNode())).level + 1;
                        // flush origin
                        c.origin.level = c.level;
                        refreshLevel(c);
                    }));
                });
                /** @type {?} */
                var child = node;
                if (child instanceof NzTreeNode) {
                    child.parentNode = _this;
                }
                else {
                    child = new NzTreeNode(node, _this);
                }
                child.level = _this.level + 1;
                child.origin.level = child.level;
                refreshLevel(child);
                try {
                    childPos === -1 ? _this.children.push(child) : _this.children.splice(childPos, 0, child);
                    // flush origin
                }
                catch (e) {
                }
            }));
            this.origin.children = this.getChildren().map((/**
             * @param {?} v
             * @return {?}
             */
            function (v) { return v.origin; }));
            // remove loading state
            this.isLoading = false;
        }
    };
    /**
     * @return {?}
     */
    NzTreeNode.prototype.clearChildren = /**
     * @return {?}
     */
    function () {
        // refresh checked state
        this.afterValueChange('clearChildren');
        this.children = [];
        this.origin.children = [];
    };
    /**
     * @return {?}
     */
    NzTreeNode.prototype.remove = /**
     * @return {?}
     */
    function () {
        var _this = this;
        /** @type {?} */
        var parentNode = this.getParentNode();
        if (parentNode) {
            parentNode.children = parentNode.getChildren().filter((/**
             * @param {?} v
             * @return {?}
             */
            function (v) { return v.key !== _this.key; }));
            parentNode.origin.children = (/** @type {?} */ (parentNode.origin.children)).filter((/**
             * @param {?} v
             * @return {?}
             */
            function (v) { return v.key !== _this.key; }));
            this.afterValueChange('remove');
        }
    };
    /**
     * @param {?} key
     * @return {?}
     */
    NzTreeNode.prototype.afterValueChange = /**
     * @param {?} key
     * @return {?}
     */
    function (key) {
        if (this.treeService) {
            switch (key) {
                case 'isChecked':
                    this.treeService.setCheckedNodeList(this);
                    break;
                case 'isHalfChecked':
                    this.treeService.setHalfCheckedNodeList(this);
                    break;
                case 'isExpanded':
                    this.treeService.setExpandedNodeList(this);
                    break;
                case 'isSelected':
                    this.treeService.setNodeActive(this);
                    break;
                case 'clearChildren':
                    this.treeService.afterRemove(this.getChildren());
                    break;
                case 'remove':
                    this.treeService.afterRemove([this]);
                    break;
            }
        }
        this.update();
    };
    /**
     * @return {?}
     */
    NzTreeNode.prototype.update = /**
     * @return {?}
     */
    function () {
        if (this.component) {
            this.component.setClassMap();
            this.component.markForCheck();
        }
    };
    return NzTreeNode;
}());
export { NzTreeNode };
if (false) {
    /**
     * @type {?}
     * @private
     */
    NzTreeNode.prototype._title;
    /** @type {?} */
    NzTreeNode.prototype.key;
    /** @type {?} */
    NzTreeNode.prototype.level;
    /** @type {?} */
    NzTreeNode.prototype.origin;
    /** @type {?} */
    NzTreeNode.prototype.parentNode;
    /**
     * @type {?}
     * @private
     */
    NzTreeNode.prototype._icon;
    /**
     * @type {?}
     * @private
     */
    NzTreeNode.prototype._children;
    /**
     * @type {?}
     * @private
     */
    NzTreeNode.prototype._isLeaf;
    /**
     * @type {?}
     * @private
     */
    NzTreeNode.prototype._isChecked;
    /**
     * @deprecated Maybe removed in next major version, use isChecked instead
     * @type {?}
     * @private
     */
    NzTreeNode.prototype._isAllChecked;
    /**
     * @type {?}
     * @private
     */
    NzTreeNode.prototype._isSelectable;
    /**
     * @type {?}
     * @private
     */
    NzTreeNode.prototype._isDisabled;
    /**
     * @type {?}
     * @private
     */
    NzTreeNode.prototype._isDisableCheckbox;
    /**
     * @type {?}
     * @private
     */
    NzTreeNode.prototype._isExpanded;
    /**
     * @type {?}
     * @private
     */
    NzTreeNode.prototype._isHalfChecked;
    /**
     * @type {?}
     * @private
     */
    NzTreeNode.prototype._isSelected;
    /**
     * @type {?}
     * @private
     */
    NzTreeNode.prototype._isLoading;
    /** @type {?} */
    NzTreeNode.prototype.isMatched;
    /** @type {?} */
    NzTreeNode.prototype.service;
    /** @type {?} */
    NzTreeNode.prototype.component;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotdHJlZS1ub2RlLmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbInRyZWUvbnotdHJlZS1ub2RlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7QUFHQSx1Q0FlQzs7O0lBZEMsa0NBQWM7O0lBQ2QsZ0NBQVk7O0lBQ1osaUNBQWM7O0lBQ2QsbUNBQWlCOztJQUNqQixvQ0FBa0I7O0lBQ2xCLHFDQUFtQjs7SUFDbkIsdUNBQXFCOztJQUNyQixxQ0FBbUI7O0lBQ25CLDRDQUEwQjs7SUFDMUIscUNBQW1COztJQUNuQixxQ0FBK0I7OztBQU1qQztJQStCRSxvQkFBWSxNQUFzQyxFQUFFLE1BQWdDLEVBQUUsT0FBd0M7UUFBMUUsdUJBQUEsRUFBQSxhQUFnQztRQUFFLHdCQUFBLEVBQUEsY0FBd0M7UUFBOUgsaUJBK0NDO1FBM0VELFVBQUssR0FBVyxDQUFDLENBQUM7UUE2QmhCLElBQUksTUFBTSxZQUFZLFVBQVUsRUFBRTtZQUNoQyxPQUFPLE1BQU0sQ0FBQztTQUNmO1FBQ0QsSUFBSSxDQUFDLE9BQU8sR0FBRyxPQUFPLElBQUksSUFBSSxDQUFDO1FBQy9CLElBQUksQ0FBQyxNQUFNLEdBQUcsTUFBTSxDQUFDO1FBQ3JCLElBQUksQ0FBQyxHQUFHLEdBQUcsTUFBTSxDQUFDLEdBQUcsSUFBSSxFQUFFLENBQUM7UUFDNUIsSUFBSSxDQUFDLFVBQVUsR0FBRyxNQUFNLENBQUM7UUFDekIsSUFBSSxDQUFDLE1BQU0sR0FBRyxNQUFNLENBQUMsS0FBSyxJQUFJLEtBQUssQ0FBQztRQUNwQyxJQUFJLENBQUMsS0FBSyxHQUFHLE1BQU0sQ0FBQyxJQUFJLElBQUksRUFBRSxDQUFDO1FBQy9CLElBQUksQ0FBQyxPQUFPLEdBQUcsTUFBTSxDQUFDLE1BQU0sSUFBSSxLQUFLLENBQUM7UUFDdEMsSUFBSSxDQUFDLFNBQVMsR0FBRyxFQUFFLENBQUM7UUFDcEIsZ0JBQWdCO1FBQ2hCLElBQUksQ0FBQyxVQUFVLEdB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O0lBQWI7UUFDRSxJQUFJLElBQUksQ0FBQyxTQUFTLEVBQUU7WUFDbEIsSUFBSSxDQUFDLFNBQVMsQ0FBQyxXQUFXLEVBQUUsQ0FBQztZQUM3QixJQUFJLENBQUMsU0FBUyxDQUFDLFlBQVksRUFBRSxDQUFDO1NBQy9CO0lBQ0gsQ0FBQztJQUNILGlCQUFDO0FBQUQsQ0FBQyxBQS9VRCxJQStVQzs7Ozs7OztJQTlVQyw0QkFBdUI7O0lBQ3ZCLHlCQUFZOztJQUNaLDJCQUFrQjs7SUFDbEIsNEJBQTBCOztJQUUxQixnQ0FBOEI7Ozs7O0lBQzlCLDJCQUFzQjs7Ozs7SUFDdEIsK0JBQWdDOzs7OztJQUNoQyw2QkFBeUI7Ozs7O0lBQ3pCLGdDQUE0Qjs7Ozs7O0lBSTVCLG1DQUErQjs7Ozs7SUFDL0IsbUNBQStCOzs7OztJQUMvQixpQ0FBNkI7Ozs7O0lBQzdCLHdDQUFvQzs7Ozs7SUFDcEMsaUNBQTZCOzs7OztJQUM3QixvQ0FBZ0M7Ozs7O0lBQ2hDLGlDQUE2Qjs7Ozs7SUFDN0IsZ0NBQTRCOztJQUM1QiwrQkFBbUI7O0lBRW5CLDZCQUFrQzs7SUFDbEMsK0JBQStCIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgTnpUcmVlQmFzZVNlcnZpY2UgfSBmcm9tICcuL256LXRyZWUtYmFzZS5zZXJ2aWNlJztcbmltcG9ydCB7IE56VHJlZU5vZGVDb21wb25lbnQgfSBmcm9tICcuL256LXRyZWUtbm9kZS5jb21wb25lbnQnO1xuXG5leHBvcnQgaW50ZXJmYWNlIE56VHJlZU5vZGVPcHRpb25zIHtcbiAgdGl0bGU6IHN0cmluZztcbiAga2V5OiBzdHJpbmc7XG4gIGljb24/OiBzdHJpbmc7XG4gIGlzTGVhZj86IGJvb2xlYW47XG4gIGNoZWNrZWQ/OiBib29sZWFuO1xuICBzZWxlY3RlZD86IGJvb2xlYW47XG4gIHNlbGVjdGFibGU/OiBib29sZWFuO1xuICBkaXNhYmxlZD86IGJvb2xlYW47XG4gIGRpc2FibGVDaGVja2JveD86IGJvb2xlYW47XG4gIGV4cGFuZGVkPzogYm9vbGVhbjtcbiAgY2hpbGRyZW4/OiBOelRyZWVOb2RlT3B0aW9uc1tdO1xuXG4gIC8vIHRzbGludDpkaXNhYmxlLW5leHQtbGluZTpuby1hbnlcbiAgW2tleTogc3RyaW5nXTogYW55O1xufVxuXG5leHBvcnQgY2xhc3MgTnpUcmVlTm9kZSB7XG4gIHByaXZhdGUgX3RpdGxlOiBzdHJpbmc7XG4gIGtleTogc3RyaW5nO1xuICBsZXZlbDogbnVtYmVyID0gMDtcbiAgb3JpZ2luOiBOelRyZWVOb2RlT3B0aW9ucztcbiAgLy8gUGFyZW50IE5vZGVcbiAgcGFyZW50Tm9kZTogTnpUcmVlTm9kZSB8IG51bGw7XG4gIHByaXZhdGUgX2ljb246IHN0cmluZztcbiAgcHJpdmF0ZSBfY2hpbGRyZW46IE56VHJlZU5vZGVbXTtcbiAgcHJpdmF0ZSBfaXNMZWFmOiBib29sZWFuO1xuICBwcml2YXRlIF9pc0NoZWNrZWQ6IGJvb2xlYW47XG4gIC8qKlxuICAgKiBAZGVwcmVjYXRlZCBNYXliZSByZW1vdmVkIGluIG5leHQgbWFqb3IgdmVyc2lvbiwgdXNlIGlzQ2hlY2tlZCBpbnN0ZWFkXG4gICAqL1xuICBwcml2YXRlIF9pc0FsbENoZWNrZWQ6IGJvb2xlYW47XG4gIHByaXZhdGUgX2lzU2VsZWN0YWJsZTogYm9vbGVhbjtcbiAgcHJpdmF0ZSBfaXNEaXNhYmxlZDogYm9vbGVhbjtcbiAgcHJpdmF0ZSBfaXNEaXNhYmxlQ2hlY2tib3g6IGJvb2xlYW47XG4gIHByaXZhdGUgX2lzRXhwYW5kZWQ6IGJvb2xlYW47XG4gIHByaXZhdGUgX2lzSGFsZkNoZWNrZWQ6IGJvb2xlYW47XG4gIHByaXZhdGUgX2lzU2VsZWN0ZWQ6IGJvb2xlYW47XG4gIHByaXZhdGUgX2lzTG9hZGluZzogYm9vbGVhbjtcbiAgaXNNYXRjaGVkOiBib29sZWFuO1xuXG4gIHNlcnZpY2U6IE56VHJlZUJhc2VTZXJ2aWNlIHwgbnVsbDtcbiAgY29tcG9uZW50OiBOelRyZWVOb2RlQ29tcG9uZW50O1xuXG4gIGdldCB0cmVlU2VydmljZSgpOiBOelRyZWVCYXNlU2VydmljZSB8IG51bGwge1xuICAgIHJldHVybiB0aGlzLnNlcnZpY2UgfHwgKHRoaXMucGFyZW50Tm9kZSAmJiB0aGlzLnBhcmVudE5vZGUudHJlZVNlcnZpY2UpO1xuICB9XG5cbiAgY29uc3RydWN0b3Iob3B0aW9uOiBOelRyZWVOb2RlT3B0aW9ucyB8IE56VHJlZU5vZGUsIHBhcmVudDogTnpUcmVlTm9kZSB8IG51bGwgPSBudWxsLCBzZXJ2aWNlOiBOelRyZWVCYXNlU2VydmljZSB8IG51bGwgPSBudWxsKSB7XG4gICAgaWYgKG9wdGlvbiBpbnN0YW5jZW9mIE56VHJlZU5vZGUpIHtcbiAgICAgIHJldHVybiBvcHRpb247XG4gICAgfVxuICAgIHRoaXMuc2VydmljZSA9IHNlcnZpY2UgfHwgbnVsbDtcbiAgICB0aGlzLm9yaWdpbiA9IG9wdGlvbjtcbiAgICB0aGlzLmtleSA9IG9wdGlvbi5rZXkgfHwgJyc7XG4gICAgdGhpcy5wYXJlbnROb2RlID0gcGFyZW50O1xuICAgIHRoaXMuX3RpdGxlID0gb3B0aW9uLnRpdGxlIHx8ICctLS0nO1xuICAgIHRoaXMuX2ljb24gPSBvcHRpb24uaWNvbiB8fCAnJztcbiAgICB0aGlzLl9pc0xlYWYgPSBvcHRpb24uaXNMZWFmIHx8IGZhbHNlO1xuICAgIHRoaXMuX2NoaWxkcmVuID0gW107XG4gICAgLy8gb3B0aW9uIHBhcmFtc1xuICAgIHRoaXMuX2lzQ2hlY2tlZCA9IG9wdGlvbi5jaGVja2VkIHx8IGZhbHNlO1xuICAgIHRoaXMuX2lzU2VsZWN0YWJsZSA9IG9wdGlvbi5kaXNhYmxlZCB8fCAob3B0aW9uLnNlbGVjdGFibGUgPT09IGZhbHNlID8gZmFsc2UgOiB0cnVlKTtcbiAgICB0aGlzLl9pc0Rpc2FibGVkID0gb3B0aW9uLmRpc2FibGVkIHx8IGZhbHNlO1xuICAgIHRoaXMuX2lzRGlzYWJsZUNoZWNrYm94ID0gb3B0aW9uLmRpc2FibGVDaGVja2JveCB8fCBmYWxzZTtcbiAgICB0aGlzLl9pc0V4cGFuZGVkID0gb3B0aW9uLmlzTGVhZiA/IGZhbHNlIDogb3B0aW9uLmV4cGFuZGVkIHx8IGZhbHNlO1xuICAgIHRoaXMuX2lzSGFsZkNoZWNrZWQgPSBmYWxzZTtcbiAgICB0aGlzLl9pc1NlbGVjdGVkID0gKCFvcHRpb24uZGlzYWJsZWQgJiYgb3B0aW9uLnNlbGVjdGVkKSB8fCBmYWxzZTtcbiAgICB0aGlzLl9pc0xvYWRpbmcgPSBmYWxzZTtcbiAgICB0aGlzLmlzTWF0Y2hlZCA9IGZhbHNlO1xuXG4gICAgLyoqXG4gICAgICogcGFyZW50J3MgY2hlY2tlZCBzdGF0dXMgd2lsbCBhZmZlY3QgY2hpbGRyZW4gd2hpbGUgaW5pdGlhbGl6aW5nXG4gICAgICovXG4gICAgaWYgKHBhcmVudCkge1xuICAgICAgdGhpcy5sZXZlbCA9IHBhcmVudC5sZXZlbCArIDE7XG4gICAgfSBlbHNlIHtcbiAgICAgIHRoaXMubGV2ZWwgPSAwO1xuICAgIH1cbiAgICBpZiAodHlwZW9mIG9wdGlvbi5jaGlsZHJlbiAhPT0gJ3VuZGVmaW5lZCcgJiYgb3B0aW9uLmNoaWxkcmVuICE9PSBudWxsKSB7XG4gICAgICBvcHRpb24uY2hpbGRyZW4uZm9yRWFjaChub2RlT3B0aW9ucyA9PiB7XG4gICAgICAgIGNvbnN0IHMgPSB0aGlzLnRyZWVTZXJ2aWNlO1xuICAgICAgICBpZiAoXG4gICAgICAgICAgcyAmJlxuICAgICAgICAgICFzLmlzQ2hlY2tTdHJpY3RseSAmJlxuICAgICAgICAgIG9wdGlvbi5jaGVja2VkICYmXG4gICAgICAgICAgIW9wdGlvbi5kaXNhYmxlZCAmJlxuICAgICAgICAgICFub2RlT3B0aW9ucy5kaXNhYmxlZCAmJlxuICAgICAgICAgICFub2RlT3B0aW9ucy5kaXNhYmxlQ2hlY2tib3hcbiAgICAgICAgKSB7XG4gICAgICAgICAgbm9kZU9wdGlvbnMuY2hlY2tlZCA9IG9wdGlvbi5jaGVja2VkO1xuICAgICAgICB9XG4gICAgICAgIHRoaXMuX2NoaWxkcmVuLnB1c2gobmV3IE56VHJlZU5vZGUobm9kZU9wdGlvbnMsIHRoaXMpKTtcbiAgICAgIH0pO1xuICAgIH1cbiAgfVxuXG4gIC8qKlxuICAgKiBhdXRvIGdlbmVyYXRlXG4gICAqIGdldFxuICAgKiBzZXRcbiAgICovXG4gIGdldCB0aXRsZSgpOiBzdHJpbmcge1xuICAgIHJldHVybiB0aGlzLl90aXRsZTtcbiAgfVxuXG4gIHNldCB0aXRsZSh2YWx1ZTogc3RyaW5nKSB7XG4gICAgdGhpcy5fdGl0bGUgPSB2YWx1ZTtcbiAgICB0aGlzLnVwZGF0ZSgpO1xuICB9XG5cbiAgZ2V0IGljb24oKTogc3RyaW5nIHtcbiAgICByZXR1cm4gdGhpcy5faWNvbjtcbiAgfVxuXG4gIHNldCBpY29uKHZhbHVlOiBzdHJpbmcpIHtcbiAgICB0aGlzLl9pY29uID0gdmFsdWU7XG4gICAgdGhpcy51cGRhdGUoKTtcbiAgfVxuXG4gIGdldCBjaGlsZHJlbigpOiBOelRyZWVOb2RlW10ge1xuICAgIHJldHVybiB0aGlzLl9jaGlsZHJlbjtcbiAgfVxuXG4gIHNldCBjaGlsZHJlbih2YWx1ZTogTnpUcmVlTm9kZVtdKSB7XG4gICAgdGhpcy5fY2hpbGRyZW4gPSB2YWx1ZTtcbiAgICB0aGlzLnVwZGF0ZSgpO1xuICB9XG5cbiAgZ2V0IGlzTGVhZigpOiBib29sZWFuIHtcbiAgICByZXR1cm4gdGhpcy5faXNMZWFmO1xuICB9XG5cbiAgc2V0IGlzTGVhZih2YWx1ZTogYm9vbGVhbikge1xuICAgIHRoaXMuX2lzTGVhZiA9IHZhbHVlO1xuICAgIC8vIHRoaXMudXBkYXRlKCk7XG4gIH1cblxuICBnZXQgaXNDaGVja2VkKCk6IGJvb2xlYW4ge1xuICAgIHJldHVybiB0aGlzLl9pc0NoZWNrZWQ7XG4gIH1cblxuICBzZXQgaXNDaGVja2VkKHZhbHVlOiBib29sZWFuKSB7XG4gICAgdGhpcy5faXNDaGVja2VkID0gdmFsdWU7XG4gICAgdGhpcy5faXNBbGxDaGVja2VkID0gdmFsdWU7XG4gICAgdGhpcy5vcmlnaW4uY2hlY2tlZCA9IHZhbHVlO1xuICAgIHRoaXMuYWZ0ZXJWYWx1ZUNoYW5nZSgnaXNDaGVja2VkJyk7XG4gIH1cblxuICBnZXQgaXNBbGxDaGVja2VkKCk6IGJvb2xlYW4ge1xuICAgIHJldHVybiB0aGlzLl9pc0FsbENoZWNrZWQ7XG4gIH1cblxuICAvKipcbiAgICogQGRlcHJlY2F0ZWQgTWF5YmUgcmVtb3ZlZCBpbiBuZXh0IG1ham9yIHZlcnNpb24sIHVzZSBpc0NoZWNrZWQgaW5zdGVhZFxuICAgKi9cbiAgc2V0IGlzQWxsQ2hlY2tlZCh2YWx1ZTogYm9vbGVhbikge1xuICAgIHRoaXMuX2lzQWxsQ2hlY2tlZCA9IHZhbHVlO1xuICB9XG5cbiAgZ2V0IGlzSGFsZkNoZWNrZWQoKTogYm9vbGVhbiB7XG4gICAgcmV0dXJuIHRoaXMuX2lzSGFsZkNoZWNrZWQ7XG4gIH1cblxuICBzZXQgaXNIYWxmQ2hlY2tlZCh2YWx1ZTogYm9vbGVhbikge1xuICAgIHRoaXMuX2lzSGFsZkNoZWNrZWQgPSB2YWx1ZTtcbiAgICB0aGlzLmFmdGVyVmFsdWVDaGFuZ2UoJ2lzSGFsZkNoZWNrZWQnKTtcbiAgfVxuXG4gIGdldCBpc1NlbGVjdGFibGUoKTogYm9vbGVhbiB7XG4gICAgcmV0dXJuIHRoaXMuX2lzU2VsZWN0YWJsZTtcbiAgfVxuXG4gIHNldCBpc1NlbGVjdGFibGUodmFsdWU6IGJvb2xlYW4pIHtcbiAgICB0aGlzLl9pc1NlbGVjdGFibGUgPSB2YWx1ZTtcbiAgICB0aGlzLnVwZGF0ZSgpO1xuICB9XG5cbiAgZ2V0IGlzRGlzYWJsZWQoKTogYm9vbGVhbiB7XG4gICAgcmV0dXJuIHRoaXMuX2lzRGlzYWJsZWQ7XG4gIH1cblxuICBzZXQgaXNEaXNhYmxlZCh2YWx1ZTogYm9vbGVhbikge1xuICAgIHRoaXMuX2lzRGlzYWJsZWQgPSB2YWx1ZTtcbiAgICB0aGlzLnVwZGF0ZSgpO1xuICB9XG5cbiAgZ2V0IGlzRGlzYWJsZUNoZWNrYm94KCk6IGJvb2xlYW4ge1xuICAgIHJldHVybiB0aGlzLl9pc0Rpc2FibGVDaGVja2JveDtcbiAgfVxuXG4gIHNldCBpc0Rpc2FibGVDaGVja2JveCh2YWx1ZTogYm9vbGVhbikge1xuICAgIHRoaXMuX2lzRGlzYWJsZUNoZWNrYm94ID0gdmFsdWU7XG4gICAgdGhpcy51cGRhdGUoKTtcbiAgfVxuXG4gIGdldCBpc0V4cGFuZGVkKCk6IGJvb2xlYW4ge1xuICAgIHJldHVybiB0aGlzLl9pc0V4cGFuZGVkO1xuICB9XG5cbiAgc2V0IGlzRXhwYW5kZWQodmFsdWU6IGJvb2xlYW4pIHtcbiAgICB0aGlzLl9pc0V4cGFuZGVkID0gdmFsdWU7XG4gICAgdGhpcy5vcmlnaW4uZXhwYW5kZWQgPSB2YWx1ZTtcbiAgICB0aGlzLmFmdGVyVmFsdWVDaGFuZ2UoJ2lzRXhwYW5kZWQnKTtcbiAgfVxuXG4gIGdldCBpc1NlbGVjdGVkKCk6IGJvb2xlYW4ge1xuICAgIHJldHVybiB0aGlzLl9pc1NlbGVjdGVkO1xuICB9XG5cbiAgc2V0IGlzU2VsZWN0ZWQodmFsdWU6IGJvb2xlYW4pIHtcbiAgICB0aGlzLl9pc1NlbGVjdGVkID0gdmFsdWU7XG4gICAgdGhpcy5vcmlnaW4uc2VsZWN0ZWQgPSB2YWx1ZTtcbiAgICB0aGlzLmFmdGVyVmFsdWVDaGFuZ2UoJ2lzU2VsZWN0ZWQnKTtcbiAgfVxuXG4gIGdldCBpc0xvYWRpbmcoKTogYm9vbGVhbiB7XG4gICAgcmV0dXJuIHRoaXMuX2lzTG9hZGluZztcbiAgfVxuXG4gIHNldCBpc0xvYWRpbmcodmFsdWU6IGJvb2xlYW4pIHtcbiAgICB0aGlzLl9pc0xvYWRpbmcgPSB2YWx1ZTtcbiAgICB0aGlzLnVwZGF0ZSgpO1xuICB9XG5cbiAgcHVibGljIHNldFN5bmNDaGVja2VkKGNoZWNrZWQ6IGJvb2xlYW4gPSBmYWxzZSwgaGFsZkNoZWNrZWQ6IGJvb2xlYW4gPSBmYWxzZSk6IHZvaWQge1xuICAgIHRoaXMuc2V0Q2hlY2tlZChjaGVja2VkLCBoYWxmQ2hlY2tlZCk7XG4gICAgaWYgKHRoaXMudHJlZVNlcnZpY2UgJiYgIXRoaXMudHJlZVNlcnZpY2UuaXNDaGVja1N0cmljdGx5KSB7XG4gICAgICB0aGlzLnRyZWVTZXJ2aWNlLmNvbmR1Y3QodGhpcyk7XG4gICAgfVxuICB9XG5cbiAgLyoqXG4gICAqIEBkZXByZWNhdGVkIE1heWJlIHJlbW92ZWQgaW4gbmV4dCBtYWpvciB2ZXJzaW9uLCB1c2UgaXNDaGVja2VkIGluc3RlYWRcbiAgICovXG4gIHB1YmxpYyBzZXRDaGVja2VkKGNoZWNrZWQ6IGJvb2xlYW4gPSBmYWxzZSwgaGFsZkNoZWNrZWQ6IGJvb2xlYW4gPSBmYWxzZSk6IHZvaWQge1xuICAgIHRoaXMub3JpZ2luLmNoZWNrZWQgPSBjaGVja2VkO1xuICAgIHRoaXMuaXNDaGVja2VkID0gY2hlY2tlZDtcbiAgICB0aGlzLmlzQWxsQ2hlY2tlZCA9IGNoZWNrZWQ7XG4gICAgdGhpcy5pc0hhbGZDaGVja2VkID0gaGFsZkNoZWNrZWQ7XG4gIH1cblxuICAvKipcbiAgICogQGRlcHJlY2F0ZWQgTWF5YmUgcmVtb3ZlZCBpbiBuZXh0IG1ham9yIHZlcnNpb24sIHVzZSBpc0V4cGFuZGVkIGluc3RlYWRcbiAgICovXG4gIHB1YmxpYyBzZXRFeHBhbmRlZCh2YWx1ZTogYm9vbGVhbik6IHZvaWQge1xuICAgIHRoaXMuaXNFeHBhbmRlZCA9IHZhbHVlO1xuICB9XG5cbiAgLyoqXG4gICAqIEBkZXByZWNhdGVkIE1heWJlIHJlbW92ZWQgaW4gbmV4dCBtYWpvciB2ZXJzaW9uLCB1c2UgaXNTZWxlY3RlZCBpbnN0ZWFkXG4gICAqL1xuICBwdWJsaWMgc2V0U2VsZWN0ZWQodmFsdWU6IGJvb2xlYW4pOiB2b2lkIHtcbiAgICBpZiAodGhpcy5pc0Rpc2FibGVkKSB7XG4gICAgICByZXR1cm47XG4gICAgfVxuICAgIHRoaXMuaXNTZWxlY3RlZCA9IHZhbHVlO1xuICB9XG5cbiAgcHVibGljIGdldFBhcmVudE5vZGUoKTogTnpUcmVlTm9kZSB8IG51bGwge1xuICAgIHJldHVybiB0aGlzLnBhcmVudE5vZGU7XG4gIH1cblxuICBwdWJsaWMgZ2V0Q2hpbGRyZW4oKTogTnpUcmVlTm9kZVtdIHtcbiAgICByZXR1cm4gdGhpcy5jaGlsZHJlbjtcbiAgfVxuXG4gIC8qKlxuICAgKiDmlK/mjIHmjInntKLlvJXkvY3nva7mj5LlhaUs5Y+25a2Q6IqC54K55LiN5Y+v5re75YqgXG4gICAqL1xuICAvLyB0c2xpbnQ6ZGlzYWJsZS1uZXh0LWxpbmU6bm8tYW55XG4gIHB1YmxpYyBhZGRDaGlsZHJlbihjaGlsZHJlbjogYW55W10sIGNoaWxkUG9zOiBudW1iZXIgPSAtMSk6IHZvaWQge1xuICAgIGlmICghdGhpcy5pc0xlYWYpIHtcbiAgICAgIGNoaWxkcmVuLmZvckVhY2goXG4gICAgICAgIChub2RlKSA9PiB7XG4gICAgICAgICAgY29uc3QgcmVmcmVzaExldmVsID0gKG46IE56VHJlZU5vZGUpID0+IHtcbiAgICAgICAgICAgIG4uZ2V0Q2hpbGRyZW4oKS5mb3JFYWNoKGMgPT4ge1xuICAgICAgICAgICAgICBjLmxldmVsID0gYy5nZXRQYXJlbnROb2RlKCkhLmxldmVsICsgMTtcbiAgICAgICAgICAgICAgLy8gZmx1c2ggb3JpZ2luXG4gICAgICAgICAgICAgIGMub3JpZ2luLmxldmVsID0gYy5sZXZlbDtcbiAgICAgICAgICAgICAgcmVmcmVzaExldmVsKGMpO1xuICAgICAgICAgICAgfSk7XG4gICAgICAgICAgfTtcbiAgICAgICAgICBsZXQgY2hpbGQgPSBub2RlO1xuICAgICAgICAgIGlmIChjaGlsZCBpbnN0YW5jZW9mIE56VHJlZU5vZGUpIHtcbiAgICAgICAgICAgIGNoaWxkLnBhcmVudE5vZGUgPSB0aGlzO1xuICAgICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICBjaGlsZCA9IG5ldyBOelRyZWVOb2RlKG5vZGUsIHRoaXMpO1xuICAgICAgICAgIH1cbiAgICAgICAgICBjaGlsZC5sZXZlbCA9IHRoaXMubGV2ZWwgKyAxO1xuICAgICAgICAgIGNoaWxkLm9yaWdpbi5sZXZlbCA9IGNoaWxkLmxldmVsO1xuICAgICAgICAgIHJlZnJlc2hMZXZlbChjaGlsZCk7XG4gICAgICAgICAgdHJ5IHtcbiAgICAgICAgICAgIGNoaWxkUG9zID09PSAtMSA/IHRoaXMuY2hpbGRyZW4ucHVzaChjaGlsZCkgOiB0aGlzLmNoaWxkcmVuLnNwbGljZShjaGlsZFBvcywgMCwgY2hpbGQpO1xuICAgICAgICAgICAgLy8gZmx1c2ggb3JpZ2luXG4gICAgICAgICAgfSBjYXRjaCAoZSkge1xuICAgICAgICAgIH1cbiAgICAgICAgfSk7XG4gICAgICB0aGlzLm9yaWdpbi5jaGlsZHJlbiA9IHRoaXMuZ2V0Q2hpbGRyZW4oKS5tYXAodiA9PiB2Lm9yaWdpbik7XG4gICAgICAvLyByZW1vdmUgbG9hZGluZyBzdGF0ZVxuICAgICAgdGhpcy5pc0xvYWRpbmcgPSBmYWxzZTtcbiAgICB9XG4gIH1cblxuICBwdWJsaWMgY2xlYXJDaGlsZHJlbigpOiB2b2lkIHtcbiAgICAvLyByZWZyZXNoIGNoZWNrZWQgc3RhdGVcbiAgICB0aGlzLmFmdGVyVmFsdWVDaGFuZ2UoJ2NsZWFyQ2hpbGRyZW4nKTtcbiAgICB0aGlzLmNoaWxkcmVuID0gW107XG4gICAgdGhpcy5vcmlnaW4uY2hpbGRyZW4gPSBbXTtcbiAgfVxuXG4gIHB1YmxpYyByZW1vdmUoKTogdm9pZCB7XG4gICAgY29uc3QgcGFyZW50Tm9kZSA9IHRoaXMuZ2V0UGFyZW50Tm9kZSgpO1xuICAgIGlmIChwYXJlbnROb2RlKSB7XG4gICAgICBwYXJlbnROb2RlLmNoaWxkcmVuID0gcGFyZW50Tm9kZS5nZXRDaGlsZHJlbigpLmZpbHRlcih2ID0+IHYua2V5ICE9PSB0aGlzLmtleSk7XG4gICAgICBwYXJlbnROb2RlLm9yaWdpbi5jaGlsZHJlbiA9IHBhcmVudE5vZGUub3JpZ2luLmNoaWxkcmVuIS5maWx0ZXIodiA9PiB2LmtleSAhPT0gdGhpcy5rZXkpO1xuICAgICAgdGhpcy5hZnRlclZhbHVlQ2hhbmdlKCdyZW1vdmUnKTtcbiAgICB9XG4gIH1cblxuICBwdWJsaWMgYWZ0ZXJWYWx1ZUNoYW5nZShrZXk6IHN0cmluZyk6IHZvaWQge1xuICAgIGlmICh0aGlzLnRyZWVTZXJ2aWNlKSB7XG4gICAgICBzd2l0Y2ggKGtleSkge1xuICAgICAgICBjYXNlICdpc0NoZWNrZWQnOlxuICAgICAgICAgIHRoaXMudHJlZVNlcnZpY2Uuc2V0Q2hlY2tlZE5vZGVMaXN0KHRoaXMpO1xuICAgICAgICAgIGJyZWFrO1xuICAgICAgICBjYXNlICdpc0hhbGZDaGVja2VkJzpcbiAgICAgICAgICB0aGlzLnRyZWVTZXJ2aWNlLnNldEhhbGZDaGVja2VkTm9kZUxpc3QodGhpcyk7XG4gICAgICAgICAgYnJlYWs7XG4gICAgICAgIGNhc2UgJ2lzRXhwYW5kZWQnOlxuICAgICAgICAgIHRoaXMudHJlZVNlcnZpY2Uuc2V0RXhwYW5kZWROb2RlTGlzdCh0aGlzKTtcbiAgICAgICAgICBicmVhaztcbiAgICAgICAgY2FzZSAnaXNTZWxlY3RlZCc6XG4gICAgICAgICAgdGhpcy50cmVlU2VydmljZS5zZXROb2RlQWN0aXZlKHRoaXMpO1xuICAgICAgICAgIGJyZWFrO1xuICAgICAgICBjYXNlICdjbGVhckNoaWxkcmVuJzpcbiAgICAgICAgICB0aGlzLnRyZWVTZXJ2aWNlLmFmdGVyUmVtb3ZlKHRoaXMuZ2V0Q2hpbGRyZW4oKSk7XG4gICAgICAgICAgYnJlYWs7XG4gICAgICAgIGNhc2UgJ3JlbW92ZSc6XG4gICAgICAgICAgdGhpcy50cmVlU2VydmljZS5hZnRlclJlbW92ZShbIHRoaXMgXSk7XG4gICAgICAgICAgYnJlYWs7XG4gICAgICB9XG4gICAgfVxuICAgIHRoaXMudXBkYXRlKCk7XG4gIH1cblxuICBwdWJsaWMgdXBkYXRlKCk6IHZvaWQge1xuICAgIGlmICh0aGlzLmNvbXBvbmVudCkge1xuICAgICAgdGhpcy5jb21wb25lbnQuc2V0Q2xhc3NNYXAoKTtcbiAgICAgIHRoaXMuY29tcG9uZW50Lm1hcmtGb3JDaGVjaygpO1xuICAgIH1cbiAgfVxufVxuIl19