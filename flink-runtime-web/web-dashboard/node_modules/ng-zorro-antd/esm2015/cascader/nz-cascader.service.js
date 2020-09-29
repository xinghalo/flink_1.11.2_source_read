/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { Injectable } from '@angular/core';
import { BehaviorSubject, Subject } from 'rxjs';
import { arraysEqual } from '../core/util/array';
import { isShowSearchObject } from './nz-cascader-definitions';
import { isChildOption, isParentOption } from './nz-cascader-utils';
/**
 * All data is stored and parsed in NzCascaderService.
 */
export class NzCascaderService {
    constructor() {
        /**
         * Activated options in each column.
         */
        this.activatedOptions = [];
        /**
         * An array to store cascader items arranged in different layers.
         */
        this.columns = [[]];
        /**
         * If user has entered searching mode.
         */
        this.inSearchingMode = false;
        /**
         * Selected options would be output to user.
         */
        this.selectedOptions = [];
        this.values = []; // tslint:disable-line:no-any
        // tslint:disable-line:no-any
        this.$loading = new BehaviorSubject(false);
        /**
         * Emit an event to notify cascader it needs to redraw because activated or
         * selected options are changed.
         */
        this.$redraw = new Subject();
        /**
         * Emit an event when an option gets selected.
         * Emit true if a leaf options is selected.
         */
        this.$optionSelected = new Subject();
        /**
         * Emit an event to notify cascader it needs to quit searching mode.
         * Only emit when user do select a searching option.
         */
        this.$quitSearching = new Subject();
        /**
         * To hold columns before entering searching mode.
         */
        this.columnsSnapshot = [[]];
        /**
         * To hold activated options before entering searching mode.
         */
        this.activatedOptionsSnapshot = [];
    }
    /**
     * Return cascader options in the first layer.
     * @return {?}
     */
    get nzOptions() {
        return this.columns[0];
    }
    /**
     * @return {?}
     */
    ngOnDestroy() {
        this.$redraw.complete();
        this.$quitSearching.complete();
        this.$optionSelected.complete();
        this.$loading.complete();
    }
    /**
     * Make sure that value matches what is displayed in the dropdown.
     * @param {?=} first
     * @return {?}
     */
    syncOptions(first = false) {
        /** @type {?} */
        const values = this.values;
        /** @type {?} */
        const hasValue = values && values.length;
        /** @type {?} */
        const lastColumnIndex = values.length - 1;
        /** @type {?} */
        const initColumnWithIndex = (/**
         * @param {?} columnIndex
         * @return {?}
         */
        (columnIndex) => {
            /** @type {?} */
            const activatedOptionSetter = (/**
             * @return {?}
             */
            () => {
                /** @type {?} */
                const currentValue = values[columnIndex];
                if (!currentValue) {
                    this.$redraw.next();
                    return;
                }
                /** @type {?} */
                const option = this.findOptionWithValue(columnIndex, values[columnIndex]) ||
                    (typeof currentValue === 'object'
                        ? currentValue
                        : {
                            [`${this.cascaderComponent.nzValueProperty}`]: currentValue,
                            [`${this.cascaderComponent.nzLabelProperty}`]: currentValue
                        });
                this.setOptionActivated(option, columnIndex, false, false);
                if (columnIndex < lastColumnIndex) {
                    initColumnWithIndex(columnIndex + 1);
                }
                else {
                    this.dropBehindColumns(columnIndex);
                    this.selectedOptions = [...this.activatedOptions];
                    this.$redraw.next();
                }
            });
            if (this.isLoaded(columnIndex) || !this.cascaderComponent.nzLoadData) {
                activatedOptionSetter();
            }
            else {
                /** @type {?} */
                const option = this.activatedOptions[columnIndex - 1] || {};
                this.loadChildren(option, columnIndex - 1, activatedOptionSetter);
            }
        });
        this.activatedOptions = [];
        this.selectedOptions = [];
        if (first && this.cascaderComponent.nzLoadData && !hasValue) {
            return;
        }
        else {
            initColumnWithIndex(0);
        }
    }
    /**
     * Bind cascader component so this service could use inputs.
     * @param {?} cascaderComponent
     * @return {?}
     */
    withComponent(cascaderComponent) {
        this.cascaderComponent = cascaderComponent;
    }
    /**
     * Reset all options. Rebuild searching options if in searching mode.
     * @param {?} options
     * @return {?}
     */
    withOptions(options) {
        this.columnsSnapshot = this.columns = options && options.length ? [options] : [];
        if (this.inSearchingMode) {
            this.prepareSearchOptions(this.cascaderComponent.inputValue);
        }
        else if (this.columns.length) {
            this.syncOptions();
        }
    }
    /**
     * Try to set a option as activated.
     * @param {?} option Cascader option
     * @param {?} columnIndex Of which column this option is in
     * @param {?=} select Select
     * @param {?=} loadingChildren Try to load children asynchronously.
     * @return {?}
     */
    setOptionActivated(option, columnIndex, select = false, loadingChildren = true) {
        if (option.disabled) {
            return;
        }
        this.activatedOptions[columnIndex] = option;
        this.trackAncestorActivatedOptions(columnIndex);
        this.dropBehindActivatedOptions(columnIndex);
        /** @type {?} */
        const isParent = isParentOption(option);
        if (isParent) {
            // Parent option that has children.
            this.setColumnData((/** @type {?} */ (option.children)), columnIndex + 1, option);
        }
        else if (!option.isLeaf && loadingChildren) {
            // Parent option that should try to load children asynchronously.
            this.loadChildren(option, columnIndex);
        }
        else if (option.isLeaf) {
            // Leaf option.
            this.dropBehindColumns(columnIndex);
        }
        // Actually perform selection to make an options not only activated but also selected.
        if (select) {
            this.setOptionSelected(option, columnIndex);
        }
        this.$redraw.next();
    }
    /**
     * Set a searching option as activated, finishing up things.
     * @param {?} option
     * @return {?}
     */
    setSearchOptionSelected(option) {
        this.activatedOptions = [option];
        this.selectedOptions = [...option.path];
        this.prepareEmitValue();
        this.$redraw.next();
        this.$optionSelected.next({ option, index: 0 });
        setTimeout((/**
         * @return {?}
         */
        () => {
            // Reset data and tell UI only to remove input and reset dropdown width style.
            this.$quitSearching.next();
            this.$redraw.next();
            this.inSearchingMode = false;
            this.columns = [...this.columnsSnapshot];
            this.activatedOptions = [...this.selectedOptions];
        }), 200);
    }
    /**
     * Filter cascader options to reset `columns`.
     * @param {?} searchValue The string user wants to search.
     * @return {?}
     */
    prepareSearchOptions(searchValue) {
        /** @type {?} */
        const results = [];
        // Search results only have one layer.
        /** @type {?} */
        const path = [];
        /** @type {?} */
        const defaultFilter = (/**
         * @param {?} i
         * @param {?} p
         * @return {?}
         */
        (i, p) => {
            return p.some((/**
             * @param {?} o
             * @return {?}
             */
            o => {
                /** @type {?} */
                const label = this.getOptionLabel(o);
                return !!label && label.indexOf(i) !== -1;
            }));
        });
        /** @type {?} */
        const showSearch = this.cascaderComponent.nzShowSearch;
        /** @type {?} */
        const filter = isShowSearchObject(showSearch) && showSearch.filter ? showSearch.filter : defaultFilter;
        /** @type {?} */
        const sorter = isShowSearchObject(showSearch) && showSearch.sorter ? showSearch.sorter : null;
        /** @type {?} */
        const loopChild = (/**
         * @param {?} node
         * @param {?=} forceDisabled
         * @return {?}
         */
        (node, forceDisabled = false) => {
            path.push(node);
            /** @type {?} */
            const cPath = Array.from(path);
            if (filter(searchValue, cPath)) {
                /** @type {?} */
                const disabled = forceDisabled || node.disabled;
                /** @type {?} */
                const option = {
                    disabled,
                    isLeaf: true,
                    path: cPath,
                    [this.cascaderComponent.nzLabelProperty]: cPath.map((/**
                     * @param {?} p
                     * @return {?}
                     */
                    p => this.getOptionLabel(p))).join(' / ')
                };
                results.push(option);
            }
            path.pop();
        });
        /** @type {?} */
        const loopParent = (/**
         * @param {?} node
         * @param {?=} forceDisabled
         * @return {?}
         */
        (node, forceDisabled = false) => {
            /** @type {?} */
            const disabled = forceDisabled || node.disabled;
            path.push(node);
            (/** @type {?} */ (node.children)).forEach((/**
             * @param {?} sNode
             * @return {?}
             */
            sNode => {
                if (!sNode.parent) {
                    sNode.parent = node;
                }
                if (!sNode.isLeaf) {
                    loopParent(sNode, disabled);
                }
                if (sNode.isLeaf || !sNode.children || !sNode.children.length) {
                    loopChild(sNode, disabled);
                }
            }));
            path.pop();
        });
        if (!this.columnsSnapshot.length) {
            this.columns = [[]];
            return;
        }
        this.columnsSnapshot[0].forEach((/**
         * @param {?} o
         * @return {?}
         */
        o => (isChildOption(o) ? loopChild(o) : loopParent(o))));
        if (sorter) {
            results.sort((/**
             * @param {?} a
             * @param {?} b
             * @return {?}
             */
            (a, b) => sorter(a.path, b.path, searchValue)));
        }
        this.columns = [results];
    }
    /**
     * Toggle searching mode by UI. It deals with things not directly related to UI.
     * @param {?} toSearching If this cascader is entering searching mode
     * @return {?}
     */
    toggleSearchingMode(toSearching) {
        this.inSearchingMode = toSearching;
        if (toSearching) {
            this.activatedOptionsSnapshot = [...this.activatedOptions];
            this.activatedOptions = [];
            this.selectedOptions = [];
            this.$redraw.next();
        }
        else {
            // User quit searching mode without selecting an option.
            this.activatedOptions = [...this.activatedOptionsSnapshot];
            this.selectedOptions = [...this.activatedOptions];
            this.columns = [...this.columnsSnapshot];
            this.syncOptions();
            this.$redraw.next();
        }
    }
    /**
     * @param {?} option
     * @param {?} index
     * @return {?}
     */
    setOptionSelected(option, index) {
        /** @type {?} */
        const changeOn = this.cascaderComponent.nzChangeOn;
        /** @type {?} */
        const shouldPerformSelection = (/**
         * @param {?} o
         * @param {?} i
         * @return {?}
         */
        (o, i) => {
            return typeof changeOn === 'function' ? changeOn(o, i) : false;
        });
        if (option.isLeaf || this.cascaderComponent.nzChangeOnSelect || shouldPerformSelection(option, index)) {
            this.selectedOptions = [...this.activatedOptions];
            this.prepareEmitValue();
            this.$redraw.next();
            this.$optionSelected.next({ option, index });
        }
    }
    /**
     * Clear selected options.
     * @return {?}
     */
    clear() {
        this.values = [];
        this.selectedOptions = [];
        this.activatedOptions = [];
        this.dropBehindColumns(0);
        this.prepareEmitValue();
        this.$redraw.next();
        this.$optionSelected.next(null);
    }
    /**
     * @param {?} o
     * @return {?}
     */
    getOptionLabel(o) {
        return (/** @type {?} */ (o[this.cascaderComponent.nzLabelProperty || 'label']));
    }
    // tslint:disable-next-line:no-any
    /**
     * @param {?} o
     * @return {?}
     */
    getOptionValue(o) {
        return o[this.cascaderComponent.nzValueProperty || 'value'];
    }
    /**
     * Try to insert options into a column.
     * @private
     * @param {?} options Options to insert
     * @param {?} columnIndex Position
     * @param {?} parent
     * @return {?}
     */
    setColumnData(options, columnIndex, parent) {
        /** @type {?} */
        const existingOptions = this.columns[columnIndex];
        if (!arraysEqual(existingOptions, options)) {
            options.forEach((/**
             * @param {?} o
             * @return {?}
             */
            o => (o.parent = parent)));
            this.columns[columnIndex] = options;
            this.dropBehindColumns(columnIndex);
        }
    }
    /**
     * Set all ancestor options as activated.
     * @private
     * @param {?} startIndex
     * @return {?}
     */
    trackAncestorActivatedOptions(startIndex) {
        for (let i = startIndex - 1; i >= 0; i--) {
            if (!this.activatedOptions[i]) {
                this.activatedOptions[i] = (/** @type {?} */ (this.activatedOptions[i + 1].parent));
            }
        }
    }
    /**
     * @private
     * @param {?} lastReserveIndex
     * @return {?}
     */
    dropBehindActivatedOptions(lastReserveIndex) {
        this.activatedOptions = this.activatedOptions.splice(0, lastReserveIndex + 1);
    }
    /**
     * @private
     * @param {?} lastReserveIndex
     * @return {?}
     */
    dropBehindColumns(lastReserveIndex) {
        if (lastReserveIndex < this.columns.length - 1) {
            this.columns = this.columns.slice(0, lastReserveIndex + 1);
        }
    }
    /**
     * Load children of an option asynchronously.
     * @param {?} option
     * @param {?} columnIndex
     * @param {?=} success
     * @param {?=} failure
     * @return {?}
     */
    loadChildren(option, // tslint:disable-line:no-any
    columnIndex, success, failure) {
        /** @type {?} */
        const loadFn = this.cascaderComponent.nzLoadData;
        if (loadFn) {
            // If there isn't any option in columns.
            this.$loading.next(columnIndex < 0);
            if (typeof option === 'object') {
                option.loading = true;
            }
            loadFn(option, columnIndex).then((/**
             * @return {?}
             */
            () => {
                option.loading = false;
                if (option.children) {
                    this.setColumnData(option.children, columnIndex + 1, option);
                }
                if (success) {
                    success();
                }
                this.$loading.next(false);
                this.$redraw.next();
            }), (/**
             * @return {?}
             */
            () => {
                option.loading = false;
                option.isLeaf = true;
                if (failure) {
                    failure();
                }
                this.$redraw.next();
            }));
        }
    }
    /**
     * @private
     * @param {?} index
     * @return {?}
     */
    isLoaded(index) {
        return this.columns[index] && this.columns[index].length > 0;
    }
    /**
     * Find a option that has a given value in a given column.
     * @private
     * @param {?} columnIndex
     * @param {?} value
     * @return {?}
     */
    findOptionWithValue(columnIndex, value // tslint:disable-line:no-any
    ) {
        /** @type {?} */
        const targetColumn = this.columns[columnIndex];
        if (targetColumn) {
            /** @type {?} */
            const v = typeof value === 'object' ? this.getOptionValue(value) : value;
            return (/** @type {?} */ (targetColumn.find((/**
             * @param {?} o
             * @return {?}
             */
            o => v === this.getOptionValue(o)))));
        }
        return null;
    }
    /**
     * @private
     * @return {?}
     */
    prepareEmitValue() {
        this.values = this.selectedOptions.map((/**
         * @param {?} o
         * @return {?}
         */
        o => this.getOptionValue(o)));
    }
}
NzCascaderService.decorators = [
    { type: Injectable }
];
if (false) {
    /**
     * Activated options in each column.
     * @type {?}
     */
    NzCascaderService.prototype.activatedOptions;
    /**
     * An array to store cascader items arranged in different layers.
     * @type {?}
     */
    NzCascaderService.prototype.columns;
    /**
     * If user has entered searching mode.
     * @type {?}
     */
    NzCascaderService.prototype.inSearchingMode;
    /**
     * Selected options would be output to user.
     * @type {?}
     */
    NzCascaderService.prototype.selectedOptions;
    /** @type {?} */
    NzCascaderService.prototype.values;
    /** @type {?} */
    NzCascaderService.prototype.$loading;
    /**
     * Emit an event to notify cascader it needs to redraw because activated or
     * selected options are changed.
     * @type {?}
     */
    NzCascaderService.prototype.$redraw;
    /**
     * Emit an event when an option gets selected.
     * Emit true if a leaf options is selected.
     * @type {?}
     */
    NzCascaderService.prototype.$optionSelected;
    /**
     * Emit an event to notify cascader it needs to quit searching mode.
     * Only emit when user do select a searching option.
     * @type {?}
     */
    NzCascaderService.prototype.$quitSearching;
    /**
     * To hold columns before entering searching mode.
     * @type {?}
     * @private
     */
    NzCascaderService.prototype.columnsSnapshot;
    /**
     * To hold activated options before entering searching mode.
     * @type {?}
     * @private
     */
    NzCascaderService.prototype.activatedOptionsSnapshot;
    /**
     * @type {?}
     * @private
     */
    NzCascaderService.prototype.cascaderComponent;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotY2FzY2FkZXIuc2VydmljZS5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJjYXNjYWRlci9uei1jYXNjYWRlci5zZXJ2aWNlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7QUFBQSxPQUFPLEVBQUUsVUFBVSxFQUFhLE1BQU0sZUFBZSxDQUFDO0FBQ3RELE9BQU8sRUFBRSxlQUFlLEVBQUUsT0FBTyxFQUFFLE1BQU0sTUFBTSxDQUFDO0FBQ2hELE9BQU8sRUFBRSxXQUFXLEVBQUUsTUFBTSxvQkFBb0IsQ0FBQztBQUVqRCxPQUFPLEVBQ0wsa0JBQWtCLEVBS25CLE1BQU0sMkJBQTJCLENBQUM7QUFDbkMsT0FBTyxFQUFFLGFBQWEsRUFBRSxjQUFjLEVBQUUsTUFBTSxxQkFBcUIsQ0FBQzs7OztBQU1wRSxNQUFNLE9BQU8saUJBQWlCO0lBRDlCOzs7O1FBR0UscUJBQWdCLEdBQXFCLEVBQUUsQ0FBQzs7OztRQUd4QyxZQUFPLEdBQXVCLENBQUMsRUFBRSxDQUFDLENBQUM7Ozs7UUFHbkMsb0JBQWUsR0FBRyxLQUFLLENBQUM7Ozs7UUFHeEIsb0JBQWUsR0FBcUIsRUFBRSxDQUFDO1FBRXZDLFdBQU0sR0FBVSxFQUFFLENBQUMsQ0FBQyw2QkFBNkI7O1FBRXhDLGFBQVEsR0FBRyxJQUFJLGVBQWUsQ0FBVSxLQUFLLENBQUMsQ0FBQzs7Ozs7UUFNL0MsWUFBTyxHQUFHLElBQUksT0FBTyxFQUFRLENBQUM7Ozs7O1FBTTlCLG9CQUFlLEdBQUcsSUFBSSxPQUFPLEVBRzNCLENBQUM7Ozs7O1FBTUgsbUJBQWMsR0FBRyxJQUFJLE9BQU8sRUFBUSxDQUFDOzs7O1FBR3RDLG9CQUFlLEdBQXVCLENBQUMsRUFBRSxDQUFDLENBQUM7Ozs7UUFHM0MsNkJBQXdCLEdBQXFCLEVBQUUsQ0FBQztJQXFYMUQsQ0FBQzs7Ozs7SUFoWEMsSUFBSSxTQUFTO1FBQ1gsT0FBTyxJQUFJLENBQUMsT0FBTyxDQUFDLENBQUMsQ0FB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QyxDQUFDO0lBQ3RFLENBQUM7OztZQS9aRixVQUFVOzs7Ozs7O0lBR1QsNkNBQXdDOzs7OztJQUd4QyxvQ0FBbUM7Ozs7O0lBR25DLDRDQUF3Qjs7Ozs7SUFHeEIsNENBQXVDOztJQUV2QyxtQ0FBbUI7O0lBRW5CLHFDQUF3RDs7Ozs7O0lBTXhELG9DQUF1Qzs7Ozs7O0lBTXZDLDRDQUdZOzs7Ozs7SUFNWiwyQ0FBOEM7Ozs7OztJQUc5Qyw0Q0FBbUQ7Ozs7OztJQUduRCxxREFBd0Q7Ozs7O0lBRXhELDhDQUF1RCIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7IEluamVjdGFibGUsIE9uRGVzdHJveSB9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuaW1wb3J0IHsgQmVoYXZpb3JTdWJqZWN0LCBTdWJqZWN0IH0gZnJvbSAncnhqcyc7XG5pbXBvcnQgeyBhcnJheXNFcXVhbCB9IGZyb20gJy4uL2NvcmUvdXRpbC9hcnJheSc7XG5cbmltcG9ydCB7XG4gIGlzU2hvd1NlYXJjaE9iamVjdCxcbiAgQ2FzY2FkZXJPcHRpb24sXG4gIENhc2NhZGVyU2VhcmNoT3B0aW9uLFxuICBOekNhc2NhZGVyQ29tcG9uZW50QXNTb3VyY2UsXG4gIE56Q2FzY2FkZXJGaWx0ZXJcbn0gZnJvbSAnLi9uei1jYXNjYWRlci1kZWZpbml0aW9ucyc7XG5pbXBvcnQgeyBpc0NoaWxkT3B0aW9uLCBpc1BhcmVudE9wdGlvbiB9IGZyb20gJy4vbnotY2FzY2FkZXItdXRpbHMnO1xuXG4vKipcbiAqIEFsbCBkYXRhIGlzIHN0b3JlZCBhbmQgcGFyc2VkIGluIE56Q2FzY2FkZXJTZXJ2aWNlLlxuICovXG5ASW5qZWN0YWJsZSgpXG5leHBvcnQgY2xhc3MgTnpDYXNjYWRlclNlcnZpY2UgaW1wbGVtZW50cyBPbkRlc3Ryb3kge1xuICAvKiogQWN0aXZhdGVkIG9wdGlvbnMgaW4gZWFjaCBjb2x1bW4uICovXG4gIGFjdGl2YXRlZE9wdGlvbnM6IENhc2NhZGVyT3B0aW9uW10gPSBbXTtcblxuICAvKiogQW4gYXJyYXkgdG8gc3RvcmUgY2FzY2FkZXIgaXRlbXMgYXJyYW5nZWQgaW4gZGlmZmVyZW50IGxheWVycy4gKi9cbiAgY29sdW1uczogQ2FzY2FkZXJPcHRpb25bXVtdID0gW1tdXTtcblxuICAvKiogSWYgdXNlciBoYXMgZW50ZXJlZCBzZWFyY2hpbmcgbW9kZS4gKi9cbiAgaW5TZWFyY2hpbmdNb2RlID0gZmFsc2U7XG5cbiAgLyoqIFNlbGVjdGVkIG9wdGlvbnMgd291bGQgYmUgb3V0cHV0IHRvIHVzZXIuICovXG4gIHNlbGVjdGVkT3B0aW9uczogQ2FzY2FkZXJPcHRpb25bXSA9IFtdO1xuXG4gIHZhbHVlczogYW55W10gPSBbXTsgLy8gdHNsaW50OmRpc2FibGUtbGluZTpuby1hbnlcblxuICByZWFkb25seSAkbG9hZGluZyA9IG5ldyBCZWhhdmlvclN1YmplY3Q8Ym9vbGVhbj4oZmFsc2UpO1xuXG4gIC8qKlxuICAgKiBFbWl0IGFuIGV2ZW50IHRvIG5vdGlmeSBjYXNjYWRlciBpdCBuZWVkcyB0byByZWRyYXcgYmVjYXVzZSBhY3RpdmF0ZWQgb3JcbiAgICogc2VsZWN0ZWQgb3B0aW9ucyBhcmUgY2hhbmdlZC5cbiAgICovXG4gIHJlYWRvbmx5ICRyZWRyYXcgPSBuZXcgU3ViamVjdDx2b2lkPigpO1xuXG4gIC8qKlxuICAgKiBFbWl0IGFuIGV2ZW50IHdoZW4gYW4gb3B0aW9uIGdldHMgc2VsZWN0ZWQuXG4gICAqIEVtaXQgdHJ1ZSBpZiBhIGxlYWYgb3B0aW9ucyBpcyBzZWxlY3RlZC5cbiAgICovXG4gIHJlYWRvbmx5ICRvcHRpb25TZWxlY3RlZCA9IG5ldyBTdWJqZWN0PHtcbiAgICBvcHRpb246IENhc2NhZGVyT3B0aW9uO1xuICAgIGluZGV4OiBudW1iZXI7XG4gIH0gfCBudWxsPigpO1xuXG4gIC8qKlxuICAgKiBFbWl0IGFuIGV2ZW50IHRvIG5vdGlmeSBjYXNjYWRlciBpdCBuZWVkcyB0byBxdWl0IHNlYXJjaGluZyBtb2RlLlxuICAgKiBPbmx5IGVtaXQgd2hlbiB1c2VyIGRvIHNlbGVjdCBhIHNlYXJjaGluZyBvcHRpb24uXG4gICAqL1xuICByZWFkb25seSAkcXVpdFNlYXJjaGluZyA9IG5ldyBTdWJqZWN0PHZvaWQ+KCk7XG5cbiAgLyoqIFRvIGhvbGQgY29sdW1ucyBiZWZvcmUgZW50ZXJpbmcgc2VhcmNoaW5nIG1vZGUuICovXG4gIHByaXZhdGUgY29sdW1uc1NuYXBzaG90OiBDYXNjYWRlck9wdGlvbltdW10gPSBbW11dO1xuXG4gIC8qKiBUbyBob2xkIGFjdGl2YXRlZCBvcHRpb25zIGJlZm9yZSBlbnRlcmluZyBzZWFyY2hpbmcgbW9kZS4gKi9cbiAgcHJpdmF0ZSBhY3RpdmF0ZWRPcHRpb25zU25hcHNob3Q6IENhc2NhZGVyT3B0aW9uW10gPSBbXTtcblxuICBwcml2YXRlIGNhc2NhZGVyQ29tcG9uZW50OiBOekNhc2NhZGVyQ29tcG9uZW50QXNTb3VyY2U7XG5cbiAgLyoqIFJldHVybiBjYXNjYWRlciBvcHRpb25zIGluIHRoZSBmaXJzdCBsYXllci4gKi9cbiAgZ2V0IG56T3B0aW9ucygpOiBDYXNjYWRlck9wdGlvbltdIHtcbiAgICByZXR1cm4gdGhpcy5jb2x1bW5zWzBdO1xuICB9XG5cbiAgbmdPbkRlc3Ryb3koKTogdm9pZCB7XG4gICAgdGhpcy4kcmVkcmF3LmNvbXBsZXRlKCk7XG4gICAgdGhpcy4kcXVpdFNlYXJjaGluZy5jb21wbGV0ZSgpO1xuICAgIHRoaXMuJG9wdGlvblNlbGVjdGVkLmNvbXBsZXRlKCk7XG4gICAgdGhpcy4kbG9hZGluZy5jb21wbGV0ZSgpO1xuICB9XG5cbiAgLyoqXG4gICAqIE1ha2Ugc3VyZSB0aGF0IHZhbHVlIG1hdGNoZXMgd2hhdCBpcyBkaXNwbGF5ZWQgaW4gdGhlIGRyb3Bkb3duLlxuICAgKi9cbiAgc3luY09wdGlvbnMoZmlyc3Q6IGJvb2xlYW4gPSBmYWxzZSk6IHZvaWQge1xuICAgIGNvbnN0IHZhbHVlcyA9IHRoaXMudmFsdWVzO1xuICAgIGNvbnN0IGhhc1ZhbHVlID0gdmFsdWVzICYmIHZhbHVlcy5sZW5ndGg7XG4gICAgY29uc3QgbGFzdENvbHVtbkluZGV4ID0gdmFsdWVzLmxlbmd0aCAtIDE7XG4gICAgY29uc3QgaW5pdENvbHVtbldpdGhJbmRleCA9IChjb2x1bW5JbmRleDogbnVtYmVyKSA9PiB7XG4gICAgICBjb25zdCBhY3RpdmF0ZWRPcHRpb25TZXR0ZXIgPSAoKSA9PiB7XG4gICAgICAgIGNvbnN0IGN1cnJlbnRWYWx1ZSA9IHZhbHVlc1tjb2x1bW5JbmRleF07XG5cbiAgICAgICAgaWYgKCFjdXJyZW50VmFsdWUpIHtcbiAgICAgICAgICB0aGlzLiRyZWRyYXcubmV4dCgpO1xuICAgICAgICAgIHJldHVybjtcbiAgICAgICAgfVxuXG4gICAgICAgIGNvbnN0IG9wdGlvbiA9XG4gICAgICAgICAgdGhpcy5maW5kT3B0aW9uV2l0aFZhbHVlKGNvbHVtbkluZGV4LCB2YWx1ZXNbY29sdW1uSW5kZXhdKSB8fFxuICAgICAgICAgICh0eXBlb2YgY3VycmVudFZhbHVlID09PSAnb2JqZWN0J1xuICAgICAgICAgICAgPyBjdXJyZW50VmFsdWVcbiAgICAgICAgICAgIDoge1xuICAgICAgICAgICAgICAgIFtgJHt0aGlzLmNhc2NhZGVyQ29tcG9uZW50Lm56VmFsdWVQcm9wZXJ0eX1gXTogY3VycmVudFZhbHVlLFxuICAgICAgICAgICAgICAgIFtgJHt0aGlzLmNhc2NhZGVyQ29tcG9uZW50Lm56TGFiZWxQcm9wZXJ0eX1gXTogY3VycmVudFZhbHVlXG4gICAgICAgICAgICAgIH0pO1xuXG4gICAgICAgIHRoaXMuc2V0T3B0aW9uQWN0aXZhdGVkKG9wdGlvbiwgY29sdW1uSW5kZXgsIGZhbHNlLCBmYWxzZSk7XG5cbiAgICAgICAgaWYgKGNvbHVtbkluZGV4IDwgbGFzdENvbHVtbkluZGV4KSB7XG4gICAgICAgICAgaW5pdENvbHVtbldpdGhJbmRleChjb2x1bW5JbmRleCArIDEpO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIHRoaXMuZHJvcEJlaGluZENvbHVtbnMoY29sdW1uSW5kZXgpO1xuICAgICAgICAgIHRoaXMuc2VsZWN0ZWRPcHRpb25zID0gWy4uLnRoaXMuYWN0aXZhdGVkT3B0aW9uc107XG4gICAgICAgICAgdGhpcy4kcmVkcmF3Lm5leHQoKTtcbiAgICAgICAgfVxuICAgICAgfTtcblxuICAgICAgaWYgKHRoaXMuaXNMb2FkZWQoY29sdW1uSW5kZXgpIHx8ICF0aGlzLmNhc2NhZGVyQ29tcG9uZW50Lm56TG9hZERhdGEpIHtcbiAgICAgICAgYWN0aXZhdGVkT3B0aW9uU2V0dGVyKCk7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICBjb25zdCBvcHRpb24gPSB0aGlzLmFjdGl2YXRlZE9wdGlvbnNbY29sdW1uSW5kZXggLSAxXSB8fCB7fTtcbiAgICAgICAgdGhpcy5sb2FkQ2hpbGRyZW4ob3B0aW9uLCBjb2x1bW5JbmRleCAtIDEsIGFjdGl2YXRlZE9wdGlvblNldHRlcik7XG4gICAgICB9XG4gICAgfTtcblxuICAgIHRoaXMuYWN0aXZhdGVkT3B0aW9ucyA9IFtdO1xuICAgIHRoaXMuc2VsZWN0ZWRPcHRpb25zID0gW107XG5cbiAgICBpZiAoZmlyc3QgJiYgdGhpcy5jYXNjYWRlckNvbXBvbmVudC5uekxvYWREYXRhICYmICFoYXNWYWx1ZSkge1xuICAgICAgcmV0dXJuO1xuICAgIH0gZWxzZSB7XG4gICAgICBpbml0Q29sdW1uV2l0aEluZGV4KDApO1xuICAgIH1cbiAgfVxuXG4gIC8qKlxuICAgKiBCaW5kIGNhc2NhZGVyIGNvbXBvbmVudCBzbyB0aGlzIHNlcnZpY2UgY291bGQgdXNlIGlucHV0cy5cbiAgICovXG4gIHdpdGhDb21wb25lbnQoY2FzY2FkZXJDb21wb25lbnQ6IE56Q2FzY2FkZXJDb21wb25lbnRBc1NvdXJjZSk6IHZvaWQge1xuICAgIHRoaXMuY2FzY2FkZXJDb21wb25lbnQgPSBjYXNjYWRlckNvbXBvbmVudDtcbiAgfVxuXG4gIC8qKlxuICAgKiBSZXNldCBhbGwgb3B0aW9ucy4gUmVidWlsZCBzZWFyY2hpbmcgb3B0aW9ucyBpZiBpbiBzZWFyY2hpbmcgbW9kZS5cbiAgICovXG4gIHdpdGhPcHRpb25zKG9wdGlvbnM6IENhc2NhZGVyT3B0aW9uW10gfCBudWxsKTogdm9pZCB7XG4gICAgdGhpcy5jb2x1bW5zU25hcHNob3QgPSB0aGlzLmNvbHVtbnMgPSBvcHRpb25zICYmIG9wdGlvbnMubGVuZ3RoID8gW29wdGlvbnNdIDogW107XG5cbiAgICBpZiAodGhpcy5pblNlYXJjaGluZ01vZGUpIHtcbiAgICAgIHRoaXMucHJlcGFyZVNlYXJjaE9wdGlvbnModGhpcy5jYXNjYWRlckNvbXBvbmVudC5pbnB1dFZhbHVlKTtcbiAgICB9IGVsc2UgaWYgKHRoaXMuY29sdW1ucy5sZW5ndGgpIHtcbiAgICAgIHRoaXMuc3luY09wdGlvbnMoKTtcbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogVHJ5IHRvIHNldCBhIG9wdGlvbiBhcyBhY3RpdmF0ZWQuXG4gICAqIEBwYXJhbSBvcHRpb24gQ2FzY2FkZXIgb3B0aW9uXG4gICAqIEBwYXJhbSBjb2x1bW5JbmRleCBPZiB3aGljaCBjb2x1bW4gdGhpcyBvcHRpb24gaXMgaW5cbiAgICogQHBhcmFtIHNlbGVjdCBTZWxlY3RcbiAgICogQHBhcmFtIGxvYWRpbmdDaGlsZHJlbiBUcnkgdG8gbG9hZCBjaGlsZHJlbiBhc3luY2hyb25vdXNseS5cbiAgICovXG4gIHNldE9wdGlvbkFjdGl2YXRlZChcbiAgICBvcHRpb246IENhc2NhZGVyT3B0aW9uLFxuICAgIGNvbHVtbkluZGV4OiBudW1iZXIsXG4gICAgc2VsZWN0OiBib29sZWFuID0gZmFsc2UsXG4gICAgbG9hZGluZ0NoaWxkcmVuOiBib29sZWFuID0gdHJ1ZVxuICApOiB2b2lkIHtcbiAgICBpZiAob3B0aW9uLmRpc2FibGVkKSB7XG4gICAgICByZXR1cm47XG4gICAgfVxuXG4gICAgdGhpcy5hY3RpdmF0ZWRPcHRpb25zW2NvbHVtbkluZGV4XSA9IG9wdGlvbjtcbiAgICB0aGlzLnRyYWNrQW5jZXN0b3JBY3RpdmF0ZWRPcHRpb25zKGNvbHVtbkluZGV4KTtcbiAgICB0aGlzLmRyb3BCZWhpbmRBY3RpdmF0ZWRPcHRpb25zKGNvbHVtbkluZGV4KTtcblxuICAgIGNvbnN0IGlzUGFyZW50ID0gaXNQYXJlbnRPcHRpb24ob3B0aW9uKTtcblxuICAgIGlmIChpc1BhcmVudCkge1xuICAgICAgLy8gUGFyZW50IG9wdGlvbiB0aGF0IGhhcyBjaGlsZHJlbi5cbiAgICAgIHRoaXMuc2V0Q29sdW1uRGF0YShvcHRpb24uY2hpbGRyZW4hLCBjb2x1bW5JbmRleCArIDEsIG9wdGlvbik7XG4gICAgfSBlbHNlIGlmICghb3B0aW9uLmlzTGVhZiAmJiBsb2FkaW5nQ2hpbGRyZW4pIHtcbiAgICAgIC8vIFBhcmVudCBvcHRpb24gdGhhdCBzaG91bGQgdHJ5IHRvIGxvYWQgY2hpbGRyZW4gYXN5bmNocm9ub3VzbHkuXG4gICAgICB0aGlzLmxvYWRDaGlsZHJlbihvcHRpb24sIGNvbHVtbkluZGV4KTtcbiAgICB9IGVsc2UgaWYgKG9wdGlvbi5pc0xlYWYpIHtcbiAgICAgIC8vIExlYWYgb3B0aW9uLlxuICAgICAgdGhpcy5kcm9wQmVoaW5kQ29sdW1ucyhjb2x1bW5JbmRleCk7XG4gICAgfVxuXG4gICAgLy8gQWN0dWFsbHkgcGVyZm9ybSBzZWxlY3Rpb24gdG8gbWFrZSBhbiBvcHRpb25zIG5vdCBvbmx5IGFjdGl2YXRlZCBidXQgYWxzbyBzZWxlY3RlZC5cbiAgICBpZiAoc2VsZWN0KSB7XG4gICAgICB0aGlzLnNldE9wdGlvblNlbGVjdGVkKG9wdGlvbiwgY29sdW1uSW5kZXgpO1xuICAgIH1cblxuICAgIHRoaXMuJHJlZHJhdy5uZXh0KCk7XG4gIH1cblxuICAvKipcbiAgICogU2V0IGEgc2VhcmNoaW5nIG9wdGlvbiBhcyBhY3RpdmF0ZWQsIGZpbmlzaGluZyB1cCB0aGluZ3MuXG4gICAqIEBwYXJhbSBvcHRpb25cbiAgICovXG4gIHNldFNlYXJjaE9wdGlvblNlbGVjdGVkKG9wdGlvbjogQ2FzY2FkZXJTZWFyY2hPcHRpb24pOiB2b2lkIHtcbiAgICB0aGlzLmFjdGl2YXRlZE9wdGlvbnMgPSBbb3B0aW9uXTtcbiAgICB0aGlzLnNlbGVjdGVkT3B0aW9ucyA9IFsuLi5vcHRpb24ucGF0aF07XG4gICAgdGhpcy5wcmVwYXJlRW1pdFZhbHVlKCk7XG4gICAgdGhpcy4kcmVkcmF3Lm5leHQoKTtcbiAgICB0aGlzLiRvcHRpb25TZWxlY3RlZC5uZXh0KHsgb3B0aW9uLCBpbmRleDogMCB9KTtcblxuICAgIHNldFRpbWVvdXQoKCkgPT4ge1xuICAgICAgLy8gUmVzZXQgZGF0YSBhbmQgdGVsbCBVSSBvbmx5IHRvIHJlbW92ZSBpbnB1dCBhbmQgcmVzZXQgZHJvcGRvd24gd2lkdGggc3R5bGUuXG4gICAgICB0aGlzLiRxdWl0U2VhcmNoaW5nLm5leHQoKTtcbiAgICAgIHRoaXMuJHJlZHJhdy5uZXh0KCk7XG4gICAgICB0aGlzLmluU2VhcmNoaW5nTW9kZSA9IGZhbHNlO1xuICAgICAgdGhpcy5jb2x1bW5zID0gWy4uLnRoaXMuY29sdW1uc1NuYXBzaG90XTtcbiAgICAgIHRoaXMuYWN0aXZhdGVkT3B0aW9ucyA9IFsuLi50aGlzLnNlbGVjdGVkT3B0aW9uc107XG4gICAgfSwgMjAwKTtcbiAgfVxuXG4gIC8qKlxuICAgKiBGaWx0ZXIgY2FzY2FkZXIgb3B0aW9ucyB0byByZXNldCBgY29sdW1uc2AuXG4gICAqIEBwYXJhbSBzZWFyY2hWYWx1ZSBUaGUgc3RyaW5nIHVzZXIgd2FudHMgdG8gc2VhcmNoLlxuICAgKi9cbiAgcHJlcGFyZVNlYXJjaE9wdGlvbnMoc2VhcmNoVmFsdWU6IHN0cmluZyk6IHZvaWQge1xuICAgIGNvbnN0IHJlc3VsdHM6IENhc2NhZGVyT3B0aW9uW10gPSBbXTsgLy8gU2VhcmNoIHJlc3VsdHMgb25seSBoYXZlIG9uZSBsYXllci5cbiAgICBjb25zdCBwYXRoOiBDYXNjYWRlck9wdGlvbltdID0gW107XG4gICAgY29uc3QgZGVmYXVsdEZpbHRlcjogTnpDYXNjYWRlckZpbHRlciA9IChpLCBwKSA9PiB7XG4gICAgICByZXR1cm4gcC5zb21lKG8gPT4ge1xuICAgICAgICBjb25zdCBsYWJlbCA9IHRoaXMuZ2V0T3B0aW9uTGFiZWwobyk7XG4gICAgICAgIHJldHVybiAhIWxhYmVsICYmIGxhYmVsLmluZGV4T2YoaSkgIT09IC0xO1xuICAgICAgfSk7XG4gICAgfTtcbiAgICBjb25zdCBzaG93U2VhcmNoID0gdGhpcy5jYXNjYWRlckNvbXBvbmVudC5uelNob3dTZWFyY2g7XG4gICAgY29uc3QgZmlsdGVyID0gaXNTaG93U2VhcmNoT2JqZWN0KHNob3dTZWFyY2gpICYmIHNob3dTZWFyY2guZmlsdGVyID8gc2hvd1NlYXJjaC5maWx0ZXIgOiBkZWZhdWx0RmlsdGVyO1xuICAgIGNvbnN0IHNvcnRlciA9IGlzU2hvd1NlYXJjaE9iamVjdChzaG93U2VhcmNoKSAmJiBzaG93U2VhcmNoLnNvcnRlciA/IHNob3dTZWFyY2guc29ydGVyIDogbnVsbDtcbiAgICBjb25zdCBsb29wQ2hpbGQgPSAobm9kZTogQ2FzY2FkZXJPcHRpb24sIGZvcmNlRGlzYWJsZWQgPSBmYWxzZSkgPT4ge1xuICAgICAgcGF0aC5wdXNoKG5vZGUpO1xuICAgICAgY29uc3QgY1BhdGggPSBBcnJheS5mcm9tKHBhdGgpO1xuICAgICAgaWYgKGZpbHRlcihzZWFyY2hWYWx1ZSwgY1BhdGgpKSB7XG4gICAgICAgIGNvbnN0IGRpc2FibGVkID0gZm9yY2VEaXNhYmxlZCB8fCBub2RlLmRpc2FibGVkO1xuICAgICAgICBjb25zdCBvcHRpb246IENhc2NhZGVyU2VhcmNoT3B0aW9uID0ge1xuICAgICAgICAgIGRpc2FibGVkLFxuICAgICAgICAgIGlzTGVhZjogdHJ1ZSxcbiAgICAgICAgICBwYXRoOiBjUGF0aCxcbiAgICAgICAgICBbdGhpcy5jYXNjYWRlckNvbXBvbmVudC5uekxhYmVsUHJvcGVydHldOiBjUGF0aC5tYXAocCA9PiB0aGlzLmdldE9wdGlvbkxhYmVsKHApKS5qb2luKCcgLyAnKVxuICAgICAgICB9O1xuICAgICAgICByZXN1bHRzLnB1c2gob3B0aW9uKTtcbiAgICAgIH1cbiAgICAgIHBhdGgucG9wKCk7XG4gICAgfTtcbiAgICBjb25zdCBsb29wUGFyZW50ID0gKG5vZGU6IENhc2NhZGVyT3B0aW9uLCBmb3JjZURpc2FibGVkID0gZmFsc2UpID0+IHtcbiAgICAgIGNvbnN0IGRpc2FibGVkID0gZm9yY2VEaXNhYmxlZCB8fCBub2RlLmRpc2FibGVkO1xuICAgICAgcGF0aC5wdXNoKG5vZGUpO1xuICAgICAgbm9kZS5jaGlsZHJlbiEuZm9yRWFjaChzTm9kZSA9PiB7XG4gICAgICAgIGlmICghc05vZGUucGFyZW50KSB7XG4gICAgICAgICAgc05vZGUucGFyZW50ID0gbm9kZTtcbiAgICAgICAgfVxuICAgICAgICBpZiAoIXNOb2RlLmlzTGVhZikge1xuICAgICAgICAgIGxvb3BQYXJlbnQoc05vZGUsIGRpc2FibGVkKTtcbiAgICAgICAgfVxuICAgICAgICBpZiAoc05vZGUuaXNMZWFmIHx8ICFzTm9kZS5jaGlsZHJlbiB8fCAhc05vZGUuY2hpbGRyZW4ubGVuZ3RoKSB7XG4gICAgICAgICAgbG9vcENoaWxkKHNOb2RlLCBkaXNhYmxlZCk7XG4gICAgICAgIH1cbiAgICAgIH0pO1xuICAgICAgcGF0aC5wb3AoKTtcbiAgICB9O1xuXG4gICAgaWYgKCF0aGlzLmNvbHVtbnNTbmFwc2hvdC5sZW5ndGgpIHtcbiAgICAgIHRoaXMuY29sdW1ucyA9IFtbXV07XG4gICAgICByZXR1cm47XG4gICAgfVxuXG4gICAgdGhpcy5jb2x1bW5zU25hcHNob3RbMF0uZm9yRWFjaChvID0+IChpc0NoaWxkT3B0aW9uKG8pID8gbG9vcENoaWxkKG8pIDogbG9vcFBhcmVudChvKSkpO1xuXG4gICAgaWYgKHNvcnRlcikge1xuICAgICAgcmVzdWx0cy5zb3J0KChhLCBiKSA9PiBzb3J0ZXIoYS5wYXRoLCBiLnBhdGgsIHNlYXJjaFZhbHVlKSk7XG4gICAgfVxuXG4gICAgdGhpcy5jb2x1bW5zID0gW3Jlc3VsdHNdO1xuICB9XG5cbiAgLyoqXG4gICAqIFRvZ2dsZSBzZWFyY2hpbmcgbW9kZSBieSBVSS4gSXQgZGVhbHMgd2l0aCB0aGluZ3Mgbm90IGRpcmVjdGx5IHJlbGF0ZWQgdG8gVUkuXG4gICAqIEBwYXJhbSB0b1NlYXJjaGluZyBJZiB0aGlzIGNhc2NhZGVyIGlzIGVudGVyaW5nIHNlYXJjaGluZyBtb2RlXG4gICAqL1xuICB0b2dnbGVTZWFyY2hpbmdNb2RlKHRvU2VhcmNoaW5nOiBib29sZWFuKTogdm9pZCB7XG4gICAgdGhpcy5pblNlYXJjaGluZ01vZGUgPSB0b1NlYXJjaGluZztcblxuICAgIGlmICh0b1NlYXJjaGluZykge1xuICAgICAgdGhpcy5hY3RpdmF0ZWRPcHRpb25zU25hcHNob3QgPSBbLi4udGhpcy5hY3RpdmF0ZWRPcHRpb25zXTtcbiAgICAgIHRoaXMuYWN0aXZhdGVkT3B0aW9ucyA9IFtdO1xuICAgICAgdGhpcy5zZWxlY3RlZE9wdGlvbnMgPSBbXTtcbiAgICAgIHRoaXMuJHJlZHJhdy5uZXh0KCk7XG4gICAgfSBlbHNlIHtcbiAgICAgIC8vIFVzZXIgcXVpdCBzZWFyY2hpbmcgbW9kZSB3aXRob3V0IHNlbGVjdGluZyBhbiBvcHRpb24uXG4gICAgICB0aGlzLmFjdGl2YXRlZE9wdGlvbnMgPSBbLi4udGhpcy5hY3RpdmF0ZWRPcHRpb25zU25hcHNob3RdO1xuICAgICAgdGhpcy5zZWxlY3RlZE9wdGlvbnMgPSBbLi4udGhpcy5hY3RpdmF0ZWRPcHRpb25zXTtcbiAgICAgIHRoaXMuY29sdW1ucyA9IFsuLi50aGlzLmNvbHVtbnNTbmFwc2hvdF07XG4gICAgICB0aGlzLnN5bmNPcHRpb25zKCk7XG4gICAgICB0aGlzLiRyZWRyYXcubmV4dCgpO1xuICAgIH1cbiAgfVxuXG4gIHNldE9wdGlvblNlbGVjdGVkKG9wdGlvbjogQ2FzY2FkZXJPcHRpb24sIGluZGV4OiBudW1iZXIpOiB2b2lkIHtcbiAgICBjb25zdCBjaGFuZ2VPbiA9IHRoaXMuY2FzY2FkZXJDb21wb25lbnQubnpDaGFuZ2VPbjtcbiAgICBjb25zdCBzaG91bGRQZXJmb3JtU2VsZWN0aW9uID0gKG86IENhc2NhZGVyT3B0aW9uLCBpOiBudW1iZXIpOiBib29sZWFuID0+IHtcbiAgICAgIHJldHVybiB0eXBlb2YgY2hhbmdlT24gPT09ICdmdW5jdGlvbicgPyBjaGFuZ2VPbihvLCBpKSA6IGZhbHNlO1xuICAgIH07XG5cbiAgICBpZiAob3B0aW9uLmlzTGVhZiB8fCB0aGlzLmNhc2NhZGVyQ29tcG9uZW50Lm56Q2hhbmdlT25TZWxlY3QgfHwgc2hvdWxkUGVyZm9ybVNlbGVjdGlvbihvcHRpb24sIGluZGV4KSkge1xuICAgICAgdGhpcy5zZWxlY3RlZE9wdGlvbnMgPSBbLi4udGhpcy5hY3RpdmF0ZWRPcHRpb25zXTtcbiAgICAgIHRoaXMucHJlcGFyZUVtaXRWYWx1ZSgpO1xuICAgICAgdGhpcy4kcmVkcmF3Lm5leHQoKTtcbiAgICAgIHRoaXMuJG9wdGlvblNlbGVjdGVkLm5leHQoeyBvcHRpb24sIGluZGV4IH0pO1xuICAgIH1cbiAgfVxuXG4gIC8qKlxuICAgKiBDbGVhciBzZWxlY3RlZCBvcHRpb25zLlxuICAgKi9cbiAgY2xlYXIoKTogdm9pZCB7XG4gICAgdGhpcy52YWx1ZXMgPSBbXTtcbiAgICB0aGlzLnNlbGVjdGVkT3B0aW9ucyA9IFtdO1xuICAgIHRoaXMuYWN0aXZhdGVkT3B0aW9ucyA9IFtdO1xuICAgIHRoaXMuZHJvcEJlaGluZENvbHVtbnMoMCk7XG4gICAgdGhpcy5wcmVwYXJlRW1pdFZhbHVlKCk7XG4gICAgdGhpcy4kcmVkcmF3Lm5leHQoKTtcbiAgICB0aGlzLiRvcHRpb25TZWxlY3RlZC5uZXh0KG51bGwpO1xuICB9XG5cbiAgZ2V0T3B0aW9uTGFiZWwobzogQ2FzY2FkZXJPcHRpb24pOiBzdHJpbmcge1xuICAgIHJldHVybiBvW3RoaXMuY2FzY2FkZXJDb21wb25lbnQubnpMYWJlbFByb3BlcnR5IHx8ICdsYWJlbCddIGFzIHN0cmluZztcbiAgfVxuXG4gIC8vIHRzbGludDpkaXNhYmxlLW5leHQtbGluZTpuby1hbnlcbiAgZ2V0T3B0aW9uVmFsdWUobzogQ2FzY2FkZXJPcHRpb24pOiBhbnkge1xuICAgIHJldHVybiBvW3RoaXMuY2FzY2FkZXJDb21wb25lbnQubnpWYWx1ZVByb3BlcnR5IHx8ICd2YWx1ZSddO1xuICB9XG5cbiAgLyoqXG4gICAqIFRyeSB0byBpbnNlcnQgb3B0aW9ucyBpbnRvIGEgY29sdW1uLlxuICAgKiBAcGFyYW0gb3B0aW9ucyBPcHRpb25zIHRvIGluc2VydFxuICAgKiBAcGFyYW0gY29sdW1uSW5kZXggUG9zaXRpb25cbiAgICovXG4gIHByaXZhdGUgc2V0Q29sdW1uRGF0YShvcHRpb25zOiBDYXNjYWRlck9wdGlvbltdLCBjb2x1bW5JbmRleDogbnVtYmVyLCBwYXJlbnQ6IENhc2NhZGVyT3B0aW9uKTogdm9pZCB7XG4gICAgY29uc3QgZXhpc3RpbmdPcHRpb25zID0gdGhpcy5jb2x1bW5zW2NvbHVtbkluZGV4XTtcbiAgICBpZiAoIWFycmF5c0VxdWFsKGV4aXN0aW5nT3B0aW9ucywgb3B0aW9ucykpIHtcbiAgICAgIG9wdGlvbnMuZm9yRWFjaChvID0+IChvLnBhcmVudCA9IHBhcmVudCkpO1xuICAgICAgdGhpcy5jb2x1bW5zW2NvbHVtbkluZGV4XSA9IG9wdGlvbnM7XG4gICAgICB0aGlzLmRyb3BCZWhpbmRDb2x1bW5zKGNvbHVtbkluZGV4KTtcbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogU2V0IGFsbCBhbmNlc3RvciBvcHRpb25zIGFzIGFjdGl2YXRlZC5cbiAgICovXG4gIHByaXZhdGUgdHJhY2tBbmNlc3RvckFjdGl2YXRlZE9wdGlvbnMoc3RhcnRJbmRleDogbnVtYmVyKTogdm9pZCB7XG4gICAgZm9yIChsZXQgaSA9IHN0YXJ0SW5kZXggLSAxOyBpID49IDA7IGktLSkge1xuICAgICAgaWYgKCF0aGlzLmFjdGl2YXRlZE9wdGlvbnNbaV0pIHtcbiAgICAgICAgdGhpcy5hY3RpdmF0ZWRPcHRpb25zW2ldID0gdGhpcy5hY3RpdmF0ZWRPcHRpb25zW2kgKyAxXS5wYXJlbnQhO1xuICAgICAgfVxuICAgIH1cbiAgfVxuXG4gIHByaXZhdGUgZHJvcEJlaGluZEFjdGl2YXRlZE9wdGlvbnMobGFzdFJlc2VydmVJbmRleDogbnVtYmVyKTogdm9pZCB7XG4gICAgdGhpcy5hY3RpdmF0ZWRPcHRpb25zID0gdGhpcy5hY3RpdmF0ZWRPcHRpb25zLnNwbGljZSgwLCBsYXN0UmVzZXJ2ZUluZGV4ICsgMSk7XG4gIH1cblxuICBwcml2YXRlIGRyb3BCZWhpbmRDb2x1bW5zKGxhc3RSZXNlcnZlSW5kZXg6IG51bWJlcik6IHZvaWQge1xuICAgIGlmIChsYXN0UmVzZXJ2ZUluZGV4IDwgdGhpcy5jb2x1bW5zLmxlbmd0aCAtIDEpIHtcbiAgICAgIHRoaXMuY29sdW1ucyA9IHRoaXMuY29sdW1ucy5zbGljZSgwLCBsYXN0UmVzZXJ2ZUluZGV4ICsgMSk7XG4gICAgfVxuICB9XG5cbiAgLyoqXG4gICAqIExvYWQgY2hpbGRyZW4gb2YgYW4gb3B0aW9uIGFzeW5jaHJvbm91c2x5LlxuICAgKi9cbiAgbG9hZENoaWxkcmVuKFxuICAgIG9wdGlvbjogQ2FzY2FkZXJPcHRpb24gfCBhbnksIC8vIHRzbGludDpkaXNhYmxlLWxpbmU6bm8tYW55XG4gICAgY29sdW1uSW5kZXg6IG51bWJlcixcbiAgICBzdWNjZXNzPzogVm9pZEZ1bmN0aW9uLFxuICAgIGZhaWx1cmU/OiBWb2lkRnVuY3Rpb25cbiAgKTogdm9pZCB7XG4gICAgY29uc3QgbG9hZEZuID0gdGhpcy5jYXNjYWRlckNvbXBvbmVudC5uekxvYWREYXRhO1xuXG4gICAgaWYgKGxvYWRGbikge1xuICAgICAgLy8gSWYgdGhlcmUgaXNuJ3QgYW55IG9wdGlvbiBpbiBjb2x1bW5zLlxuICAgICAgdGhpcy4kbG9hZGluZy5uZXh0KGNvbHVtbkluZGV4IDwgMCk7XG5cbiAgICAgIGlmICh0eXBlb2Ygb3B0aW9uID09PSAnb2JqZWN0Jykge1xuICAgICAgICBvcHRpb24ubG9hZGluZyA9IHRydWU7XG4gICAgICB9XG5cbiAgICAgIGxvYWRGbihvcHRpb24sIGNvbHVtbkluZGV4KS50aGVuKFxuICAgICAgICAoKSA9PiB7XG4gICAgICAgICAgb3B0aW9uLmxvYWRpbmcgPSBmYWxzZTtcbiAgICAgICAgICBpZiAob3B0aW9uLmNoaWxkcmVuKSB7XG4gICAgICAgICAgICB0aGlzLnNldENvbHVtbkRhdGEob3B0aW9uLmNoaWxkcmVuLCBjb2x1bW5JbmRleCArIDEsIG9wdGlvbik7XG4gICAgICAgICAgfVxuICAgICAgICAgIGlmIChzdWNjZXNzKSB7XG4gICAgICAgICAgICBzdWNjZXNzKCk7XG4gICAgICAgICAgfVxuICAgICAgICAgIHRoaXMuJGxvYWRpbmcubmV4dChmYWxzZSk7XG4gICAgICAgICAgdGhpcy4kcmVkcmF3Lm5leHQoKTtcbiAgICAgICAgfSxcbiAgICAgICAgKCkgPT4ge1xuICAgICAgICAgIG9wdGlvbi5sb2FkaW5nID0gZmFsc2U7XG4gICAgICAgICAgb3B0aW9uLmlzTGVhZiA9IHRydWU7XG4gICAgICAgICAgaWYgKGZhaWx1cmUpIHtcbiAgICAgICAgICAgIGZhaWx1cmUoKTtcbiAgICAgICAgICB9XG4gICAgICAgICAgdGhpcy4kcmVkcmF3Lm5leHQoKTtcbiAgICAgICAgfVxuICAgICAgKTtcbiAgICB9XG4gIH1cblxuICBwcml2YXRlIGlzTG9hZGVkKGluZGV4OiBudW1iZXIpOiBib29sZWFuIHtcbiAgICByZXR1cm4gdGhpcy5jb2x1bW5zW2luZGV4XSAmJiB0aGlzLmNvbHVtbnNbaW5kZXhdLmxlbmd0aCA+IDA7XG4gIH1cblxuICAvKipcbiAgICogRmluZCBhIG9wdGlvbiB0aGF0IGhhcyBhIGdpdmVuIHZhbHVlIGluIGEgZ2l2ZW4gY29sdW1uLlxuICAgKi9cbiAgcHJpdmF0ZSBmaW5kT3B0aW9uV2l0aFZhbHVlKFxuICAgIGNvbHVtbkluZGV4OiBudW1iZXIsXG4gICAgdmFsdWU6IENhc2NhZGVyT3B0aW9uIHwgYW55IC8vIHRzbGludDpkaXNhYmxlLWxpbmU6bm8tYW55XG4gICk6IENhc2NhZGVyT3B0aW9uIHwgbnVsbCB7XG4gICAgY29uc3QgdGFyZ2V0Q29sdW1uID0gdGhpcy5jb2x1bW5zW2NvbHVtbkluZGV4XTtcbiAgICBpZiAodGFyZ2V0Q29sdW1uKSB7XG4gICAgICBjb25zdCB2ID0gdHlwZW9mIHZhbHVlID09PSAnb2JqZWN0JyA/IHRoaXMuZ2V0T3B0aW9uVmFsdWUodmFsdWUpIDogdmFsdWU7XG4gICAgICByZXR1cm4gdGFyZ2V0Q29sdW1uLmZpbmQobyA9PiB2ID09PSB0aGlzLmdldE9wdGlvblZhbHVlKG8pKSE7XG4gICAgfVxuICAgIHJldHVybiBudWxsO1xuICB9XG5cbiAgcHJpdmF0ZSBwcmVwYXJlRW1pdFZhbHVlKCk6IHZvaWQge1xuICAgIHRoaXMudmFsdWVzID0gdGhpcy5zZWxlY3RlZE9wdGlvbnMubWFwKG8gPT4gdGhpcy5nZXRPcHRpb25WYWx1ZShvKSk7XG4gIH1cbn1cbiJdfQ==