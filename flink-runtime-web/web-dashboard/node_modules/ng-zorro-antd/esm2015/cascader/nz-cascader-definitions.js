/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * @record
 */
export function CascaderOption() { }
if (false) {
    /** @type {?|undefined} */
    CascaderOption.prototype.value;
    /** @type {?|undefined} */
    CascaderOption.prototype.label;
    /** @type {?|undefined} */
    CascaderOption.prototype.title;
    /** @type {?|undefined} */
    CascaderOption.prototype.disabled;
    /** @type {?|undefined} */
    CascaderOption.prototype.loading;
    /** @type {?|undefined} */
    CascaderOption.prototype.isLeaf;
    /** @type {?|undefined} */
    CascaderOption.prototype.parent;
    /** @type {?|undefined} */
    CascaderOption.prototype.children;
    /* Skipping unhandled member: [key: string]: any;*/
}
/**
 * @record
 */
export function CascaderSearchOption() { }
if (false) {
    /** @type {?} */
    CascaderSearchOption.prototype.path;
}
/**
 * @record
 */
export function NzShowSearchOptions() { }
if (false) {
    /** @type {?|undefined} */
    NzShowSearchOptions.prototype.filter;
    /** @type {?|undefined} */
    NzShowSearchOptions.prototype.sorter;
}
/**
 * @param {?} options
 * @return {?}
 */
export function isShowSearchObject(options) {
    return typeof options !== 'boolean';
}
/**
 * To avoid circular dependency, provide an interface of `NzCascaderComponent`
 * for `NzCascaderService`.
 * @record
 */
export function NzCascaderComponentAsSource() { }
if (false) {
    /** @type {?} */
    NzCascaderComponentAsSource.prototype.inputValue;
    /** @type {?} */
    NzCascaderComponentAsSource.prototype.nzShowSearch;
    /** @type {?} */
    NzCascaderComponentAsSource.prototype.nzLabelProperty;
    /** @type {?} */
    NzCascaderComponentAsSource.prototype.nzValueProperty;
    /** @type {?} */
    NzCascaderComponentAsSource.prototype.nzChangeOnSelect;
    /**
     * @param {?} option
     * @param {?} level
     * @return {?}
     */
    NzCascaderComponentAsSource.prototype.nzChangeOn = function (option, level) { };
    /**
     * @param {?} node
     * @param {?=} index
     * @return {?}
     */
    NzCascaderComponentAsSource.prototype.nzLoadData = function (node, index) { };
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotY2FzY2FkZXItZGVmaW5pdGlvbnMuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsiY2FzY2FkZXIvbnotY2FzY2FkZXItZGVmaW5pdGlvbnMudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7OztBQWVBLG9DQVdDOzs7SUFWQywrQkFBWTs7SUFDWiwrQkFBZTs7SUFDZiwrQkFBZTs7SUFDZixrQ0FBbUI7O0lBQ25CLGlDQUFrQjs7SUFDbEIsZ0NBQWlCOztJQUNqQixnQ0FBd0I7O0lBQ3hCLGtDQUE0Qjs7Ozs7O0FBSzlCLDBDQUVDOzs7SUFEQyxvQ0FBdUI7Ozs7O0FBR3pCLHlDQUdDOzs7SUFGQyxxQ0FBMEI7O0lBQzFCLHFDQUEwQjs7Ozs7O0FBRzVCLE1BQU0sVUFBVSxrQkFBa0IsQ0FDaEMsT0FBc0M7SUFFdEMsT0FBTyxPQUFPLE9BQU8sS0FBSyxTQUFTLENBQUM7QUFDdEMsQ0FBQzs7Ozs7O0FBTUQsaURBV0M7OztJQVZDLGlEQUFtQjs7SUFDbkIsbURBQTRDOztJQUM1QyxzREFBd0I7O0lBQ3hCLHNEQUF3Qjs7SUFDeEIsdURBQTBCOzs7Ozs7SUFFMUIsZ0ZBQTREOzs7Ozs7SUFHNUQsOEVBQW9FIiwic291cmNlc0NvbnRlbnQiOlsiZXhwb3J0IHR5cGUgTnpDYXNjYWRlckV4cGFuZFRyaWdnZXIgPSAnY2xpY2snIHwgJ2hvdmVyJztcbmV4cG9ydCB0eXBlIE56Q2FzY2FkZXJUcmlnZ2VyVHlwZSA9ICdjbGljaycgfCAnaG92ZXInO1xuZXhwb3J0IHR5cGUgTnpDYXNjYWRlclNpemUgPSAnc21hbGwnIHwgJ2xhcmdlJyB8ICdkZWZhdWx0JztcblxuZXhwb3J0IHR5cGUgTnpDYXNjYWRlckZpbHRlciA9IChcbiAgc2VhcmNoVmFsdWU6IHN0cmluZyxcbiAgcGF0aDogQ2FzY2FkZXJPcHRpb25bXVxuKSA9PiBib29sZWFuO1xuXG5leHBvcnQgdHlwZSBOekNhc2NhZGVyU29ydGVyID0gKFxuICBhOiBDYXNjYWRlck9wdGlvbltdLFxuICBiOiBDYXNjYWRlck9wdGlvbltdLFxuICBpbnB1dFZhbHVlOiBzdHJpbmdcbikgPT4gbnVtYmVyO1xuXG5leHBvcnQgaW50ZXJmYWNlIENhc2NhZGVyT3B0aW9uIHtcbiAgdmFsdWU/OiBhbnk7IC8vIHRzbGludDpkaXNhYmxlLWxpbmU6bm8tYW55XG4gIGxhYmVsPzogc3RyaW5nO1xuICB0aXRsZT86IHN0cmluZztcbiAgZGlzYWJsZWQ/OiBib29sZWFuO1xuICBsb2FkaW5nPzogYm9vbGVhbjtcbiAgaXNMZWFmPzogYm9vbGVhbjtcbiAgcGFyZW50PzogQ2FzY2FkZXJPcHRpb247XG4gIGNoaWxkcmVuPzogQ2FzY2FkZXJPcHRpb25bXTtcblxuICBba2V5OiBzdHJpbmddOiBhbnk7IC8vIHRzbGludDpkaXNhYmxlLWxpbmU6bm8tYW55XG59XG5cbmV4cG9ydCBpbnRlcmZhY2UgQ2FzY2FkZXJTZWFyY2hPcHRpb24gZXh0ZW5kcyBDYXNjYWRlck9wdGlvbiB7XG4gIHBhdGg6IENhc2NhZGVyT3B0aW9uW107XG59XG5cbmV4cG9ydCBpbnRlcmZhY2UgTnpTaG93U2VhcmNoT3B0aW9ucyB7XG4gIGZpbHRlcj86IE56Q2FzY2FkZXJGaWx0ZXI7XG4gIHNvcnRlcj86IE56Q2FzY2FkZXJTb3J0ZXI7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpc1Nob3dTZWFyY2hPYmplY3QoXG4gIG9wdGlvbnM6IE56U2hvd1NlYXJjaE9wdGlvbnMgfCBib29sZWFuXG4pOiBvcHRpb25zIGlzIE56U2hvd1NlYXJjaE9wdGlvbnMge1xuICByZXR1cm4gdHlwZW9mIG9wdGlvbnMgIT09ICdib29sZWFuJztcbn1cblxuLyoqXG4gKiBUbyBhdm9pZCBjaXJjdWxhciBkZXBlbmRlbmN5LCBwcm92aWRlIGFuIGludGVyZmFjZSBvZiBgTnpDYXNjYWRlckNvbXBvbmVudGBcbiAqIGZvciBgTnpDYXNjYWRlclNlcnZpY2VgLlxuICovXG5leHBvcnQgaW50ZXJmYWNlIE56Q2FzY2FkZXJDb21wb25lbnRBc1NvdXJjZSB7XG4gIGlucHV0VmFsdWU6IHN0cmluZztcbiAgbnpTaG93U2VhcmNoOiBOelNob3dTZWFyY2hPcHRpb25zIHwgYm9vbGVhbjtcbiAgbnpMYWJlbFByb3BlcnR5OiBzdHJpbmc7XG4gIG56VmFsdWVQcm9wZXJ0eTogc3RyaW5nO1xuICBuekNoYW5nZU9uU2VsZWN0OiBib29sZWFuO1xuXG4gIG56Q2hhbmdlT24/KG9wdGlvbjogQ2FzY2FkZXJPcHRpb24sIGxldmVsOiBudW1iZXIpOiBib29sZWFuO1xuXG4gIC8vIHRzbGludDpkaXNhYmxlLW5leHQtbGluZTpuby1hbnlcbiAgbnpMb2FkRGF0YT8obm9kZTogQ2FzY2FkZXJPcHRpb24sIGluZGV4PzogbnVtYmVyKTogUHJvbWlzZUxpa2U8YW55Pjtcbn1cbiJdfQ==