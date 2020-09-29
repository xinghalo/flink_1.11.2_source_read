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
export class NzTreeNode {
    /**
     * @param {?} option
     * @param {?=} parent
     * @param {?=} service
     */
    constructor(option, parent = null, service = null) {
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
            nodeOptions => {
                /** @type {?} */
                const s = this.treeService;
                if (s &&
                    !s.isCheckStrictly &&
                    option.checked &&
                    !option.disabled &&
                    !nodeOptions.disabled &&
                    !nodeOptions.disableCheckbox) {
                    nodeOptions.checked = option.checked;
                }
                this._children.push(new NzTreeNode(nodeOptions, this));
            }));
        }
    }
    /**
     * @return {?}
     */
    get treeService() {
        return this.service || (this.parentNode && this.parentNode.treeService);
    }
    /**
     * auto generate
     * get
     * set
     * @return {?}
     */
    get title() {
        return this._title;
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set title(value) {
        this._title = value;
        this.update();
    }
    /**
     * @return {?}
     */
    get icon() {
        return this._icon;
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set icon(value) {
        this._icon = value;
        this.update();
    }
    /**
     * @return {?}
     */
    get children() {
        return this._children;
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set children(value) {
        this._children = value;
        this.update();
    }
    /**
     * @return {?}
     */
    get isLeaf() {
        return this._isLeaf;
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set isLeaf(value) {
        this._isLeaf = value;
        // this.update();
    }
    /**
     * @return {?}
     */
    get isChecked() {
        return this._isChecked;
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set isChecked(value) {
        this._isChecked = value;
        this._isAllChecked = value;
        this.origin.checked = value;
        this.afterValueChange('isChecked');
    }
    /**
     * @return {?}
     */
    get isAllChecked() {
        return this._isAllChecked;
    }
    /**
     * @deprecated Maybe removed in next major version, use isChecked instead
     * @param {?} value
     * @return {?}
     */
    set isAllChecked(value) {
        this._isAllChecked = value;
    }
    /**
     * @return {?}
     */
    get isHalfChecked() {
        return this._isHalfChecked;
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set isHalfChecked(value) {
        this._isHalfChecked = value;
        this.afterValueChange('isHalfChecked');
    }
    /**
     * @return {?}
     */
    get isSelectable() {
        return this._isSelectable;
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set isSelectable(value) {
        this._isSelectable = value;
        this.update();
    }
    /**
     * @return {?}
     */
    get isDisabled() {
        return this._isDisabled;
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set isDisabled(value) {
        this._isDisabled = value;
        this.update();
    }
    /**
     * @return {?}
     */
    get isDisableCheckbox() {
        return this._isDisableCheckbox;
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set isDisableCheckbox(value) {
        this._isDisableCheckbox = value;
        this.update();
    }
    /**
     * @return {?}
     */
    get isExpanded() {
        return this._isExpanded;
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set isExpanded(value) {
        this._isExpanded = value;
        this.origin.expanded = value;
        this.afterValueChange('isExpanded');
    }
    /**
     * @return {?}
     */
    get isSelected() {
        return this._isSelected;
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set isSelected(value) {
        this._isSelected = value;
        this.origin.selected = value;
        this.afterValueChange('isSelected');
    }
    /**
     * @return {?}
     */
    get isLoading() {
        return this._isLoading;
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set isLoading(value) {
        this._isLoading = value;
        this.update();
    }
    /**
     * @param {?=} checked
     * @param {?=} halfChecked
     * @return {?}
     */
    setSyncChecked(checked = false, halfChecked = false) {
        this.setChecked(checked, halfChecked);
        if (this.treeService && !this.treeService.isCheckStrictly) {
            this.treeService.conduct(this);
        }
    }
    /**
     * @deprecated Maybe removed in next major version, use isChecked instead
     * @param {?=} checked
     * @param {?=} halfChecked
     * @return {?}
     */
    setChecked(checked = false, halfChecked = false) {
        this.origin.checked = checked;
        this.isChecked = checked;
        this.isAllChecked = checked;
        this.isHalfChecked = halfChecked;
    }
    /**
     * @deprecated Maybe removed in next major version, use isExpanded instead
     * @param {?} value
     * @return {?}
     */
    setExpanded(value) {
        this.isExpanded = value;
    }
    /**
     * @deprecated Maybe removed in next major version, use isSelected instead
     * @param {?} value
     * @return {?}
     */
    setSelected(value) {
        if (this.isDisabled) {
            return;
        }
        this.isSelected = value;
    }
    /**
     * @return {?}
     */
    getParentNode() {
        return this.parentNode;
    }
    /**
     * @return {?}
     */
    getChildren() {
        return this.children;
    }
    /**
     * 支持按索引位置插入,叶子节点不可添加
     * @param {?} children
     * @param {?=} childPos
     * @return {?}
     */
    // tslint:disable-next-line:no-any
    addChildren(children, childPos = -1) {
        if (!this.isLeaf) {
            children.forEach((/**
             * @param {?} node
             * @return {?}
             */
            (node) => {
                /** @type {?} */
                const refreshLevel = (/**
                 * @param {?} n
                 * @return {?}
                 */
                (n) => {
                    n.getChildren().forEach((/**
                     * @param {?} c
                     * @return {?}
                     */
                    c => {
                        c.level = (/** @type {?} */ (c.getParentNode())).level + 1;
                        // flush origin
                        c.origin.level = c.level;
                        refreshLevel(c);
                    }));
                });
                /** @type {?} */
                let child = node;
                if (child instanceof NzTreeNode) {
                    child.parentNode = this;
                }
                else {
                    child = new NzTreeNode(node, this);
                }
                child.level = this.level + 1;
                child.origin.level = child.level;
                refreshLevel(child);
                try {
                    childPos === -1 ? this.children.push(child) : this.children.splice(childPos, 0, child);
                    // flush origin
                }
                catch (e) {
                }
            }));
            this.origin.children = this.getChildren().map((/**
             * @param {?} v
             * @return {?}
             */
            v => v.origin));
            // remove loading state
            this.isLoading = false;
        }
    }
    /**
     * @return {?}
     */
    clearChildren() {
        // refresh checked state
        this.afterValueChange('clearChildren');
        this.children = [];
        this.origin.children = [];
    }
    /**
     * @return {?}
     */
    remove() {
        /** @type {?} */
        const parentNode = this.getParentNode();
        if (parentNode) {
            parentNode.children = parentNode.getChildren().filter((/**
             * @param {?} v
             * @return {?}
             */
            v => v.key !== this.key));
            parentNode.origin.children = (/** @type {?} */ (parentNode.origin.children)).filter((/**
             * @param {?} v
             * @return {?}
             */
            v => v.key !== this.key));
            this.afterValueChange('remove');
        }
    }
    /**
     * @param {?} key
     * @return {?}
     */
    afterValueChange(key) {
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
    }
    /**
     * @return {?}
     */
    update() {
        if (this.component) {
            this.component.setClassMap();
            this.component.markForCheck();
        }
    }
}
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotdHJlZS1ub2RlLmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbInRyZWUvbnotdHJlZS1ub2RlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7QUFHQSx1Q0FlQzs7O0lBZEMsa0NBQWM7O0lBQ2QsZ0NBQVk7O0lBQ1osaUNBQWM7O0lBQ2QsbUNBQWlCOztJQUNqQixvQ0FBa0I7O0lBQ2xCLHFDQUFtQjs7SUFDbkIsdUNBQXFCOztJQUNyQixxQ0FBbUI7O0lBQ25CLDRDQUEwQjs7SUFDMUIscUNBQW1COztJQUNuQixxQ0FBK0I7OztBQU1qQyxNQUFNLE9BQU8sVUFBVTs7Ozs7O0lBK0JyQixZQUFZLE1BQXNDLEVBQUUsU0FBNEIsSUFBSSxFQUFFLFVBQW9DLElBQUk7UUE1QjlILFVBQUssR0FBVyxDQUFDLENBQUM7UUE2QmhCLElBQUksTUFBTSxZQUFZLFVBQVUsRUFBRTtZQUNoQyxPQUFPLE1BQU0sQ0FBQztTQUNmO1FBQ0QsSUFBSSxDQUFDLE9BQU8sR0FBRyxPQUFPLElBQUksSUFBSSxDQUFDO1FBQy9CLElBQUksQ0FBQyxNQUFNLEdBQUcsTUFBTSxDQUFDO1FBQ3JCLElBQUksQ0FBQyxHQUFHLEdBQUcsTUFBTSxDQUFDLEdBQUcsSUFBSSxFQUFFLENBQUM7UUFDNUIsSUFBSSxDQUFDLFVBQVUsR0FBRyxNQUFNLENBQUM7UUFDekIsSUFBSSxDQUFDLE1BQU0sR0FBRyxNQUFNLENBQUMsS0FBSyxJQUFJLEtBQUssQ0FBQztRQUNwQyxJQUFJLENBQUMsS0FBSyxHQUFHLE1BQU0sQ0FBQyxJQUFJLElBQUksRUFBRSxDQUFDO1FBQy9CLElBQUksQ0FBQyxPQUFPLEdBQUcsTUFBTSxDQUFDLE1BQU0sSUFBSSxLQUFLLENBQUM7UUFDdEMsSUFBSSxDQUFDLFNBQVMsR0FBRyxFQUFFLENBQUM7UUFDcEIsZ0JBQWdCO1FBQ2hCLElBQUksQ0FBQyxVQUFVLEdBQUcsTUFBTSxDQUFDLE9BQU8sSUFBSSxLQUFL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Q0FBNEI7O0lBQzVCLCtCQUFtQjs7SUFFbkIsNkJBQWtDOztJQUNsQywrQkFBK0IiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgeyBOelRyZWVCYXNlU2VydmljZSB9IGZyb20gJy4vbnotdHJlZS1iYXNlLnNlcnZpY2UnO1xuaW1wb3J0IHsgTnpUcmVlTm9kZUNvbXBvbmVudCB9IGZyb20gJy4vbnotdHJlZS1ub2RlLmNvbXBvbmVudCc7XG5cbmV4cG9ydCBpbnRlcmZhY2UgTnpUcmVlTm9kZU9wdGlvbnMge1xuICB0aXRsZTogc3RyaW5nO1xuICBrZXk6IHN0cmluZztcbiAgaWNvbj86IHN0cmluZztcbiAgaXNMZWFmPzogYm9vbGVhbjtcbiAgY2hlY2tlZD86IGJvb2xlYW47XG4gIHNlbGVjdGVkPzogYm9vbGVhbjtcbiAgc2VsZWN0YWJsZT86IGJvb2xlYW47XG4gIGRpc2FibGVkPzogYm9vbGVhbjtcbiAgZGlzYWJsZUNoZWNrYm94PzogYm9vbGVhbjtcbiAgZXhwYW5kZWQ/OiBib29sZWFuO1xuICBjaGlsZHJlbj86IE56VHJlZU5vZGVPcHRpb25zW107XG5cbiAgLy8gdHNsaW50OmRpc2FibGUtbmV4dC1saW5lOm5vLWFueVxuICBba2V5OiBzdHJpbmddOiBhbnk7XG59XG5cbmV4cG9ydCBjbGFzcyBOelRyZWVOb2RlIHtcbiAgcHJpdmF0ZSBfdGl0bGU6IHN0cmluZztcbiAga2V5OiBzdHJpbmc7XG4gIGxldmVsOiBudW1iZXIgPSAwO1xuICBvcmlnaW46IE56VHJlZU5vZGVPcHRpb25zO1xuICAvLyBQYXJlbnQgTm9kZVxuICBwYXJlbnROb2RlOiBOelRyZWVOb2RlIHwgbnVsbDtcbiAgcHJpdmF0ZSBfaWNvbjogc3RyaW5nO1xuICBwcml2YXRlIF9jaGlsZHJlbjogTnpUcmVlTm9kZVtdO1xuICBwcml2YXRlIF9pc0xlYWY6IGJvb2xlYW47XG4gIHByaXZhdGUgX2lzQ2hlY2tlZDogYm9vbGVhbjtcbiAgLyoqXG4gICAqIEBkZXByZWNhdGVkIE1heWJlIHJlbW92ZWQgaW4gbmV4dCBtYWpvciB2ZXJzaW9uLCB1c2UgaXNDaGVja2VkIGluc3RlYWRcbiAgICovXG4gIHByaXZhdGUgX2lzQWxsQ2hlY2tlZDogYm9vbGVhbjtcbiAgcHJpdmF0ZSBfaXNTZWxlY3RhYmxlOiBib29sZWFuO1xuICBwcml2YXRlIF9pc0Rpc2FibGVkOiBib29sZWFuO1xuICBwcml2YXRlIF9pc0Rpc2FibGVDaGVja2JveDogYm9vbGVhbjtcbiAgcHJpdmF0ZSBfaXNFeHBhbmRlZDogYm9vbGVhbjtcbiAgcHJpdmF0ZSBfaXNIYWxmQ2hlY2tlZDogYm9vbGVhbjtcbiAgcHJpdmF0ZSBfaXNTZWxlY3RlZDogYm9vbGVhbjtcbiAgcHJpdmF0ZSBfaXNMb2FkaW5nOiBib29sZWFuO1xuICBpc01hdGNoZWQ6IGJvb2xlYW47XG5cbiAgc2VydmljZTogTnpUcmVlQmFzZVNlcnZpY2UgfCBudWxsO1xuICBjb21wb25lbnQ6IE56VHJlZU5vZGVDb21wb25lbnQ7XG5cbiAgZ2V0IHRyZWVTZXJ2aWNlKCk6IE56VHJlZUJhc2VTZXJ2aWNlIHwgbnVsbCB7XG4gICAgcmV0dXJuIHRoaXMuc2VydmljZSB8fCAodGhpcy5wYXJlbnROb2RlICYmIHRoaXMucGFyZW50Tm9kZS50cmVlU2VydmljZSk7XG4gIH1cblxuICBjb25zdHJ1Y3RvcihvcHRpb246IE56VHJlZU5vZGVPcHRpb25zIHwgTnpUcmVlTm9kZSwgcGFyZW50OiBOelRyZWVOb2RlIHwgbnVsbCA9IG51bGwsIHNlcnZpY2U6IE56VHJlZUJhc2VTZXJ2aWNlIHwgbnVsbCA9IG51bGwpIHtcbiAgICBpZiAob3B0aW9uIGluc3RhbmNlb2YgTnpUcmVlTm9kZSkge1xuICAgICAgcmV0dXJuIG9wdGlvbjtcbiAgICB9XG4gICAgdGhpcy5zZXJ2aWNlID0gc2VydmljZSB8fCBudWxsO1xuICAgIHRoaXMub3JpZ2luID0gb3B0aW9uO1xuICAgIHRoaXMua2V5ID0gb3B0aW9uLmtleSB8fCAnJztcbiAgICB0aGlzLnBhcmVudE5vZGUgPSBwYXJlbnQ7XG4gICAgdGhpcy5fdGl0bGUgPSBvcHRpb24udGl0bGUgfHwgJy0tLSc7XG4gICAgdGhpcy5faWNvbiA9IG9wdGlvbi5pY29uIHx8ICcnO1xuICAgIHRoaXMuX2lzTGVhZiA9IG9wdGlvbi5pc0xlYWYgfHwgZmFsc2U7XG4gICAgdGhpcy5fY2hpbGRyZW4gPSBbXTtcbiAgICAvLyBvcHRpb24gcGFyYW1zXG4gICAgdGhpcy5faXNDaGVja2VkID0gb3B0aW9uLmNoZWNrZWQgfHwgZmFsc2U7XG4gICAgdGhpcy5faXNTZWxlY3RhYmxlID0gb3B0aW9uLmRpc2FibGVkIHx8IChvcHRpb24uc2VsZWN0YWJsZSA9PT0gZmFsc2UgPyBmYWxzZSA6IHRydWUpO1xuICAgIHRoaXMuX2lzRGlzYWJsZWQgPSBvcHRpb24uZGlzYWJsZWQgfHwgZmFsc2U7XG4gICAgdGhpcy5faXNEaXNhYmxlQ2hlY2tib3ggPSBvcHRpb24uZGlzYWJsZUNoZWNrYm94IHx8IGZhbHNlO1xuICAgIHRoaXMuX2lzRXhwYW5kZWQgPSBvcHRpb24uaXNMZWFmID8gZmFsc2UgOiBvcHRpb24uZXhwYW5kZWQgfHwgZmFsc2U7XG4gICAgdGhpcy5faXNIYWxmQ2hlY2tlZCA9IGZhbHNlO1xuICAgIHRoaXMuX2lzU2VsZWN0ZWQgPSAoIW9wdGlvbi5kaXNhYmxlZCAmJiBvcHRpb24uc2VsZWN0ZWQpIHx8IGZhbHNlO1xuICAgIHRoaXMuX2lzTG9hZGluZyA9IGZhbHNlO1xuICAgIHRoaXMuaXNNYXRjaGVkID0gZmFsc2U7XG5cbiAgICAvKipcbiAgICAgKiBwYXJlbnQncyBjaGVja2VkIHN0YXR1cyB3aWxsIGFmZmVjdCBjaGlsZHJlbiB3aGlsZSBpbml0aWFsaXppbmdcbiAgICAgKi9cbiAgICBpZiAocGFyZW50KSB7XG4gICAgICB0aGlzLmxldmVsID0gcGFyZW50LmxldmVsICsgMTtcbiAgICB9IGVsc2Uge1xuICAgICAgdGhpcy5sZXZlbCA9IDA7XG4gICAgfVxuICAgIGlmICh0eXBlb2Ygb3B0aW9uLmNoaWxkcmVuICE9PSAndW5kZWZpbmVkJyAmJiBvcHRpb24uY2hpbGRyZW4gIT09IG51bGwpIHtcbiAgICAgIG9wdGlvbi5jaGlsZHJlbi5mb3JFYWNoKG5vZGVPcHRpb25zID0+IHtcbiAgICAgICAgY29uc3QgcyA9IHRoaXMudHJlZVNlcnZpY2U7XG4gICAgICAgIGlmIChcbiAgICAgICAgICBzICYmXG4gICAgICAgICAgIXMuaXNDaGVja1N0cmljdGx5ICYmXG4gICAgICAgICAgb3B0aW9uLmNoZWNrZWQgJiZcbiAgICAgICAgICAhb3B0aW9uLmRpc2FibGVkICYmXG4gICAgICAgICAgIW5vZGVPcHRpb25zLmRpc2FibGVkICYmXG4gICAgICAgICAgIW5vZGVPcHRpb25zLmRpc2FibGVDaGVja2JveFxuICAgICAgICApIHtcbiAgICAgICAgICBub2RlT3B0aW9ucy5jaGVja2VkID0gb3B0aW9uLmNoZWNrZWQ7XG4gICAgICAgIH1cbiAgICAgICAgdGhpcy5fY2hpbGRyZW4ucHVzaChuZXcgTnpUcmVlTm9kZShub2RlT3B0aW9ucywgdGhpcykpO1xuICAgICAgfSk7XG4gICAgfVxuICB9XG5cbiAgLyoqXG4gICAqIGF1dG8gZ2VuZXJhdGVcbiAgICogZ2V0XG4gICAqIHNldFxuICAgKi9cbiAgZ2V0IHRpdGxlKCk6IHN0cmluZyB7XG4gICAgcmV0dXJuIHRoaXMuX3RpdGxlO1xuICB9XG5cbiAgc2V0IHRpdGxlKHZhbHVlOiBzdHJpbmcpIHtcbiAgICB0aGlzLl90aXRsZSA9IHZhbHVlO1xuICAgIHRoaXMudXBkYXRlKCk7XG4gIH1cblxuICBnZXQgaWNvbigpOiBzdHJpbmcge1xuICAgIHJldHVybiB0aGlzLl9pY29uO1xuICB9XG5cbiAgc2V0IGljb24odmFsdWU6IHN0cmluZykge1xuICAgIHRoaXMuX2ljb24gPSB2YWx1ZTtcbiAgICB0aGlzLnVwZGF0ZSgpO1xuICB9XG5cbiAgZ2V0IGNoaWxkcmVuKCk6IE56VHJlZU5vZGVbXSB7XG4gICAgcmV0dXJuIHRoaXMuX2NoaWxkcmVuO1xuICB9XG5cbiAgc2V0IGNoaWxkcmVuKHZhbHVlOiBOelRyZWVOb2RlW10pIHtcbiAgICB0aGlzLl9jaGlsZHJlbiA9IHZhbHVlO1xuICAgIHRoaXMudXBkYXRlKCk7XG4gIH1cblxuICBnZXQgaXNMZWFmKCk6IGJvb2xlYW4ge1xuICAgIHJldHVybiB0aGlzLl9pc0xlYWY7XG4gIH1cblxuICBzZXQgaXNMZWFmKHZhbHVlOiBib29sZWFuKSB7XG4gICAgdGhpcy5faXNMZWFmID0gdmFsdWU7XG4gICAgLy8gdGhpcy51cGRhdGUoKTtcbiAgfVxuXG4gIGdldCBpc0NoZWNrZWQoKTogYm9vbGVhbiB7XG4gICAgcmV0dXJuIHRoaXMuX2lzQ2hlY2tlZDtcbiAgfVxuXG4gIHNldCBpc0NoZWNrZWQodmFsdWU6IGJvb2xlYW4pIHtcbiAgICB0aGlzLl9pc0NoZWNrZWQgPSB2YWx1ZTtcbiAgICB0aGlzLl9pc0FsbENoZWNrZWQgPSB2YWx1ZTtcbiAgICB0aGlzLm9yaWdpbi5jaGVja2VkID0gdmFsdWU7XG4gICAgdGhpcy5hZnRlclZhbHVlQ2hhbmdlKCdpc0NoZWNrZWQnKTtcbiAgfVxuXG4gIGdldCBpc0FsbENoZWNrZWQoKTogYm9vbGVhbiB7XG4gICAgcmV0dXJuIHRoaXMuX2lzQWxsQ2hlY2tlZDtcbiAgfVxuXG4gIC8qKlxuICAgKiBAZGVwcmVjYXRlZCBNYXliZSByZW1vdmVkIGluIG5leHQgbWFqb3IgdmVyc2lvbiwgdXNlIGlzQ2hlY2tlZCBpbnN0ZWFkXG4gICAqL1xuICBzZXQgaXNBbGxDaGVja2VkKHZhbHVlOiBib29sZWFuKSB7XG4gICAgdGhpcy5faXNBbGxDaGVja2VkID0gdmFsdWU7XG4gIH1cblxuICBnZXQgaXNIYWxmQ2hlY2tlZCgpOiBib29sZWFuIHtcbiAgICByZXR1cm4gdGhpcy5faXNIYWxmQ2hlY2tlZDtcbiAgfVxuXG4gIHNldCBpc0hhbGZDaGVja2VkKHZhbHVlOiBib29sZWFuKSB7XG4gICAgdGhpcy5faXNIYWxmQ2hlY2tlZCA9IHZhbHVlO1xuICAgIHRoaXMuYWZ0ZXJWYWx1ZUNoYW5nZSgnaXNIYWxmQ2hlY2tlZCcpO1xuICB9XG5cbiAgZ2V0IGlzU2VsZWN0YWJsZSgpOiBib29sZWFuIHtcbiAgICByZXR1cm4gdGhpcy5faXNTZWxlY3RhYmxlO1xuICB9XG5cbiAgc2V0IGlzU2VsZWN0YWJsZSh2YWx1ZTogYm9vbGVhbikge1xuICAgIHRoaXMuX2lzU2VsZWN0YWJsZSA9IHZhbHVlO1xuICAgIHRoaXMudXBkYXRlKCk7XG4gIH1cblxuICBnZXQgaXNEaXNhYmxlZCgpOiBib29sZWFuIHtcbiAgICByZXR1cm4gdGhpcy5faXNEaXNhYmxlZDtcbiAgfVxuXG4gIHNldCBpc0Rpc2FibGVkKHZhbHVlOiBib29sZWFuKSB7XG4gICAgdGhpcy5faXNEaXNhYmxlZCA9IHZhbHVlO1xuICAgIHRoaXMudXBkYXRlKCk7XG4gIH1cblxuICBnZXQgaXNEaXNhYmxlQ2hlY2tib3goKTogYm9vbGVhbiB7XG4gICAgcmV0dXJuIHRoaXMuX2lzRGlzYWJsZUNoZWNrYm94O1xuICB9XG5cbiAgc2V0IGlzRGlzYWJsZUNoZWNrYm94KHZhbHVlOiBib29sZWFuKSB7XG4gICAgdGhpcy5faXNEaXNhYmxlQ2hlY2tib3ggPSB2YWx1ZTtcbiAgICB0aGlzLnVwZGF0ZSgpO1xuICB9XG5cbiAgZ2V0IGlzRXhwYW5kZWQoKTogYm9vbGVhbiB7XG4gICAgcmV0dXJuIHRoaXMuX2lzRXhwYW5kZWQ7XG4gIH1cblxuICBzZXQgaXNFeHBhbmRlZCh2YWx1ZTogYm9vbGVhbikge1xuICAgIHRoaXMuX2lzRXhwYW5kZWQgPSB2YWx1ZTtcbiAgICB0aGlzLm9yaWdpbi5leHBhbmRlZCA9IHZhbHVlO1xuICAgIHRoaXMuYWZ0ZXJWYWx1ZUNoYW5nZSgnaXNFeHBhbmRlZCcpO1xuICB9XG5cbiAgZ2V0IGlzU2VsZWN0ZWQoKTogYm9vbGVhbiB7XG4gICAgcmV0dXJuIHRoaXMuX2lzU2VsZWN0ZWQ7XG4gIH1cblxuICBzZXQgaXNTZWxlY3RlZCh2YWx1ZTogYm9vbGVhbikge1xuICAgIHRoaXMuX2lzU2VsZWN0ZWQgPSB2YWx1ZTtcbiAgICB0aGlzLm9yaWdpbi5zZWxlY3RlZCA9IHZhbHVlO1xuICAgIHRoaXMuYWZ0ZXJWYWx1ZUNoYW5nZSgnaXNTZWxlY3RlZCcpO1xuICB9XG5cbiAgZ2V0IGlzTG9hZGluZygpOiBib29sZWFuIHtcbiAgICByZXR1cm4gdGhpcy5faXNMb2FkaW5nO1xuICB9XG5cbiAgc2V0IGlzTG9hZGluZyh2YWx1ZTogYm9vbGVhbikge1xuICAgIHRoaXMuX2lzTG9hZGluZyA9IHZhbHVlO1xuICAgIHRoaXMudXBkYXRlKCk7XG4gIH1cblxuICBwdWJsaWMgc2V0U3luY0NoZWNrZWQoY2hlY2tlZDogYm9vbGVhbiA9IGZhbHNlLCBoYWxmQ2hlY2tlZDogYm9vbGVhbiA9IGZhbHNlKTogdm9pZCB7XG4gICAgdGhpcy5zZXRDaGVja2VkKGNoZWNrZWQsIGhhbGZDaGVja2VkKTtcbiAgICBpZiAodGhpcy50cmVlU2VydmljZSAmJiAhdGhpcy50cmVlU2VydmljZS5pc0NoZWNrU3RyaWN0bHkpIHtcbiAgICAgIHRoaXMudHJlZVNlcnZpY2UuY29uZHVjdCh0aGlzKTtcbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogQGRlcHJlY2F0ZWQgTWF5YmUgcmVtb3ZlZCBpbiBuZXh0IG1ham9yIHZlcnNpb24sIHVzZSBpc0NoZWNrZWQgaW5zdGVhZFxuICAgKi9cbiAgcHVibGljIHNldENoZWNrZWQoY2hlY2tlZDogYm9vbGVhbiA9IGZhbHNlLCBoYWxmQ2hlY2tlZDogYm9vbGVhbiA9IGZhbHNlKTogdm9pZCB7XG4gICAgdGhpcy5vcmlnaW4uY2hlY2tlZCA9IGNoZWNrZWQ7XG4gICAgdGhpcy5pc0NoZWNrZWQgPSBjaGVja2VkO1xuICAgIHRoaXMuaXNBbGxDaGVja2VkID0gY2hlY2tlZDtcbiAgICB0aGlzLmlzSGFsZkNoZWNrZWQgPSBoYWxmQ2hlY2tlZDtcbiAgfVxuXG4gIC8qKlxuICAgKiBAZGVwcmVjYXRlZCBNYXliZSByZW1vdmVkIGluIG5leHQgbWFqb3IgdmVyc2lvbiwgdXNlIGlzRXhwYW5kZWQgaW5zdGVhZFxuICAgKi9cbiAgcHVibGljIHNldEV4cGFuZGVkKHZhbHVlOiBib29sZWFuKTogdm9pZCB7XG4gICAgdGhpcy5pc0V4cGFuZGVkID0gdmFsdWU7XG4gIH1cblxuICAvKipcbiAgICogQGRlcHJlY2F0ZWQgTWF5YmUgcmVtb3ZlZCBpbiBuZXh0IG1ham9yIHZlcnNpb24sIHVzZSBpc1NlbGVjdGVkIGluc3RlYWRcbiAgICovXG4gIHB1YmxpYyBzZXRTZWxlY3RlZCh2YWx1ZTogYm9vbGVhbik6IHZvaWQge1xuICAgIGlmICh0aGlzLmlzRGlzYWJsZWQpIHtcbiAgICAgIHJldHVybjtcbiAgICB9XG4gICAgdGhpcy5pc1NlbGVjdGVkID0gdmFsdWU7XG4gIH1cblxuICBwdWJsaWMgZ2V0UGFyZW50Tm9kZSgpOiBOelRyZWVOb2RlIHwgbnVsbCB7XG4gICAgcmV0dXJuIHRoaXMucGFyZW50Tm9kZTtcbiAgfVxuXG4gIHB1YmxpYyBnZXRDaGlsZHJlbigpOiBOelRyZWVOb2RlW10ge1xuICAgIHJldHVybiB0aGlzLmNoaWxkcmVuO1xuICB9XG5cbiAgLyoqXG4gICAqIOaUr+aMgeaMiee0ouW8leS9jee9ruaPkuWFpSzlj7blrZDoioLngrnkuI3lj6/mt7vliqBcbiAgICovXG4gIC8vIHRzbGludDpkaXNhYmxlLW5leHQtbGluZTpuby1hbnlcbiAgcHVibGljIGFkZENoaWxkcmVuKGNoaWxkcmVuOiBhbnlbXSwgY2hpbGRQb3M6IG51bWJlciA9IC0xKTogdm9pZCB7XG4gICAgaWYgKCF0aGlzLmlzTGVhZikge1xuICAgICAgY2hpbGRyZW4uZm9yRWFjaChcbiAgICAgICAgKG5vZGUpID0+IHtcbiAgICAgICAgICBjb25zdCByZWZyZXNoTGV2ZWwgPSAobjogTnpUcmVlTm9kZSkgPT4ge1xuICAgICAgICAgICAgbi5nZXRDaGlsZHJlbigpLmZvckVhY2goYyA9PiB7XG4gICAgICAgICAgICAgIGMubGV2ZWwgPSBjLmdldFBhcmVudE5vZGUoKSEubGV2ZWwgKyAxO1xuICAgICAgICAgICAgICAvLyBmbHVzaCBvcmlnaW5cbiAgICAgICAgICAgICAgYy5vcmlnaW4ubGV2ZWwgPSBjLmxldmVsO1xuICAgICAgICAgICAgICByZWZyZXNoTGV2ZWwoYyk7XG4gICAgICAgICAgICB9KTtcbiAgICAgICAgICB9O1xuICAgICAgICAgIGxldCBjaGlsZCA9IG5vZGU7XG4gICAgICAgICAgaWYgKGNoaWxkIGluc3RhbmNlb2YgTnpUcmVlTm9kZSkge1xuICAgICAgICAgICAgY2hpbGQucGFyZW50Tm9kZSA9IHRoaXM7XG4gICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgIGNoaWxkID0gbmV3IE56VHJlZU5vZGUobm9kZSwgdGhpcyk7XG4gICAgICAgICAgfVxuICAgICAgICAgIGNoaWxkLmxldmVsID0gdGhpcy5sZXZlbCArIDE7XG4gICAgICAgICAgY2hpbGQub3JpZ2luLmxldmVsID0gY2hpbGQubGV2ZWw7XG4gICAgICAgICAgcmVmcmVzaExldmVsKGNoaWxkKTtcbiAgICAgICAgICB0cnkge1xuICAgICAgICAgICAgY2hpbGRQb3MgPT09IC0xID8gdGhpcy5jaGlsZHJlbi5wdXNoKGNoaWxkKSA6IHRoaXMuY2hpbGRyZW4uc3BsaWNlKGNoaWxkUG9zLCAwLCBjaGlsZCk7XG4gICAgICAgICAgICAvLyBmbHVzaCBvcmlnaW5cbiAgICAgICAgICB9IGNhdGNoIChlKSB7XG4gICAgICAgICAgfVxuICAgICAgICB9KTtcbiAgICAgIHRoaXMub3JpZ2luLmNoaWxkcmVuID0gdGhpcy5nZXRDaGlsZHJlbigpLm1hcCh2ID0+IHYub3JpZ2luKTtcbiAgICAgIC8vIHJlbW92ZSBsb2FkaW5nIHN0YXRlXG4gICAgICB0aGlzLmlzTG9hZGluZyA9IGZhbHNlO1xuICAgIH1cbiAgfVxuXG4gIHB1YmxpYyBjbGVhckNoaWxkcmVuKCk6IHZvaWQge1xuICAgIC8vIHJlZnJlc2ggY2hlY2tlZCBzdGF0ZVxuICAgIHRoaXMuYWZ0ZXJWYWx1ZUNoYW5nZSgnY2xlYXJDaGlsZHJlbicpO1xuICAgIHRoaXMuY2hpbGRyZW4gPSBbXTtcbiAgICB0aGlzLm9yaWdpbi5jaGlsZHJlbiA9IFtdO1xuICB9XG5cbiAgcHVibGljIHJlbW92ZSgpOiB2b2lkIHtcbiAgICBjb25zdCBwYXJlbnROb2RlID0gdGhpcy5nZXRQYXJlbnROb2RlKCk7XG4gICAgaWYgKHBhcmVudE5vZGUpIHtcbiAgICAgIHBhcmVudE5vZGUuY2hpbGRyZW4gPSBwYXJlbnROb2RlLmdldENoaWxkcmVuKCkuZmlsdGVyKHYgPT4gdi5rZXkgIT09IHRoaXMua2V5KTtcbiAgICAgIHBhcmVudE5vZGUub3JpZ2luLmNoaWxkcmVuID0gcGFyZW50Tm9kZS5vcmlnaW4uY2hpbGRyZW4hLmZpbHRlcih2ID0+IHYua2V5ICE9PSB0aGlzLmtleSk7XG4gICAgICB0aGlzLmFmdGVyVmFsdWVDaGFuZ2UoJ3JlbW92ZScpO1xuICAgIH1cbiAgfVxuXG4gIHB1YmxpYyBhZnRlclZhbHVlQ2hhbmdlKGtleTogc3RyaW5nKTogdm9pZCB7XG4gICAgaWYgKHRoaXMudHJlZVNlcnZpY2UpIHtcbiAgICAgIHN3aXRjaCAoa2V5KSB7XG4gICAgICAgIGNhc2UgJ2lzQ2hlY2tlZCc6XG4gICAgICAgICAgdGhpcy50cmVlU2VydmljZS5zZXRDaGVja2VkTm9kZUxpc3QodGhpcyk7XG4gICAgICAgICAgYnJlYWs7XG4gICAgICAgIGNhc2UgJ2lzSGFsZkNoZWNrZWQnOlxuICAgICAgICAgIHRoaXMudHJlZVNlcnZpY2Uuc2V0SGFsZkNoZWNrZWROb2RlTGlzdCh0aGlzKTtcbiAgICAgICAgICBicmVhaztcbiAgICAgICAgY2FzZSAnaXNFeHBhbmRlZCc6XG4gICAgICAgICAgdGhpcy50cmVlU2VydmljZS5zZXRFeHBhbmRlZE5vZGVMaXN0KHRoaXMpO1xuICAgICAgICAgIGJyZWFrO1xuICAgICAgICBjYXNlICdpc1NlbGVjdGVkJzpcbiAgICAgICAgICB0aGlzLnRyZWVTZXJ2aWNlLnNldE5vZGVBY3RpdmUodGhpcyk7XG4gICAgICAgICAgYnJlYWs7XG4gICAgICAgIGNhc2UgJ2NsZWFyQ2hpbGRyZW4nOlxuICAgICAgICAgIHRoaXMudHJlZVNlcnZpY2UuYWZ0ZXJSZW1vdmUodGhpcy5nZXRDaGlsZHJlbigpKTtcbiAgICAgICAgICBicmVhaztcbiAgICAgICAgY2FzZSAncmVtb3ZlJzpcbiAgICAgICAgICB0aGlzLnRyZWVTZXJ2aWNlLmFmdGVyUmVtb3ZlKFsgdGhpcyBdKTtcbiAgICAgICAgICBicmVhaztcbiAgICAgIH1cbiAgICB9XG4gICAgdGhpcy51cGRhdGUoKTtcbiAgfVxuXG4gIHB1YmxpYyB1cGRhdGUoKTogdm9pZCB7XG4gICAgaWYgKHRoaXMuY29tcG9uZW50KSB7XG4gICAgICB0aGlzLmNvbXBvbmVudC5zZXRDbGFzc01hcCgpO1xuICAgICAgdGhpcy5jb21wb25lbnQubWFya0ZvckNoZWNrKCk7XG4gICAgfVxuICB9XG59XG4iXX0=