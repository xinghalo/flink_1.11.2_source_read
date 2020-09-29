/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { Pipe } from '@angular/core';
var NzFilterOptionPipe = /** @class */ (function () {
    function NzFilterOptionPipe() {
    }
    /**
     * @param {?} options
     * @param {?} searchValue
     * @param {?} filterOption
     * @param {?} serverSearch
     * @return {?}
     */
    NzFilterOptionPipe.prototype.transform = /**
     * @param {?} options
     * @param {?} searchValue
     * @param {?} filterOption
     * @param {?} serverSearch
     * @return {?}
     */
    function (options, searchValue, filterOption, serverSearch) {
        if (serverSearch || !searchValue) {
            return options;
        }
        else {
            return ((/** @type {?} */ (options))).filter((/**
             * @param {?} o
             * @return {?}
             */
            function (o) { return filterOption(searchValue, o); }));
        }
    };
    NzFilterOptionPipe.decorators = [
        { type: Pipe, args: [{ name: 'nzFilterOption' },] }
    ];
    return NzFilterOptionPipe;
}());
export { NzFilterOptionPipe };
var NzFilterGroupOptionPipe = /** @class */ (function () {
    function NzFilterGroupOptionPipe() {
    }
    /**
     * @param {?} groups
     * @param {?} searchValue
     * @param {?} filterOption
     * @param {?} serverSearch
     * @return {?}
     */
    NzFilterGroupOptionPipe.prototype.transform = /**
     * @param {?} groups
     * @param {?} searchValue
     * @param {?} filterOption
     * @param {?} serverSearch
     * @return {?}
     */
    function (groups, searchValue, filterOption, serverSearch) {
        if (serverSearch || !searchValue) {
            return groups;
        }
        else {
            return ((/** @type {?} */ (groups))).filter((/**
             * @param {?} g
             * @return {?}
             */
            function (g) {
                return g.listOfNzOptionComponent.some((/**
                 * @param {?} o
                 * @return {?}
                 */
                function (o) { return filterOption(searchValue, o); }));
            }));
        }
    };
    NzFilterGroupOptionPipe.decorators = [
        { type: Pipe, args: [{ name: 'nzFilterGroupOption' },] }
    ];
    return NzFilterGroupOptionPipe;
}());
export { NzFilterGroupOptionPipe };
/**
 * @param {?} searchValue
 * @param {?} option
 * @return {?}
 */
export function defaultFilterOption(searchValue, option) {
    if (option && option.nzLabel) {
        return option.nzLabel.toLowerCase().indexOf(searchValue.toLowerCase()) > -1;
    }
    else {
        return false;
    }
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotb3B0aW9uLnBpcGUuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsic2VsZWN0L256LW9wdGlvbi5waXBlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7QUFBQSxPQUFPLEVBQUUsSUFBSSxFQUFpQixNQUFNLGVBQWUsQ0FBQztBQU1wRDtJQUFBO0lBY0EsQ0FBQzs7Ozs7Ozs7SUFaQyxzQ0FBUzs7Ozs7OztJQUFULFVBQ0UsT0FBNEIsRUFDNUIsV0FBbUIsRUFDbkIsWUFBMkIsRUFDM0IsWUFBcUI7UUFFckIsSUFBSSxZQUFZLElBQUksQ0FBQyxXQUFXLEVBQUU7WUFDaEMsT0FBTyxPQUFPLENBQUM7U0FDaEI7YUFBTTtZQUNMLE9BQU8sQ0FBQyxtQkFBQSxPQUFPLEVBQXVCLENBQUMsQ0FBQyxNQUFNOzs7O1lBQUMsVUFBQSxDQUFDLElBQUksT0FBQSxZQUFZLENBQUMsV0FBVyxFQUFFLENBQUMsQ0FBQyxFQUE1QixDQUE0QixFQUFDLENBQUM7U0FDbkY7SUFDSCxDQUFDOztnQkFiRixJQUFJLFNBQUMsRUFBRSxJQUFJLEVBQUUsZ0JBQWdCLEVBQUU7O0lBY2hDLHlCQUFDO0NBQUEsQUFkRCxJQWNDO1NBYlksa0JBQWtCO0FBZS9CO0lBQUE7SUFnQkEsQ0FBQzs7Ozs7Ozs7SUFkQywyQ0FBUzs7Ozs7OztJQUFULFVBQ0UsTUFBZ0MsRUFDaEMsV0FBbUIsRUFDbkIsWUFBMkIsRUFDM0IsWUFBcUI7UUFFckIsSUFBSSxZQUFZLElBQUksQ0FBQyxXQUFXLEVBQUU7WUFDaEMsT0FBTyxNQUFNLENBQUM7U0FDZjthQUFNO1lBQ0wsT0FBTyxDQUFDLG1CQUFBLE1BQU0sRUFBNEIsQ0FBQyxDQUFDLE1BQU07Ozs7WUFBQyxVQUFBLENBQUM7Z0JBQ2xELE9BQU8sQ0FBQyxDQUFDLHVCQUF1QixDQUFDLElBQUk7Ozs7Z0JBQUMsVUFBQSxDQUFDLElBQUksT0FBQSxZQUFZLENBQUMsV0FBVyxFQUFFLENBQUMsQ0FBQyxFQUE1QixDQUE0QixFQUFDLENBQUM7WUFDM0UsQ0FBQyxFQUFDLENBQUM7U0FDSjtJQUNILENBQUM7O2dCQWZGLElBQUksU0FBQyxFQUFFLElBQUksRUFBRSxxQkFBcUIsRUFBRTs7SUFnQnJDLDhCQUFDO0NBQUEsQUFoQkQsSUFnQkM7U0FmWSx1QkFBdUI7Ozs7OztBQWlCcEMsTUFBTSxVQUFVLG1CQUFtQixDQUFDLFdBQW1CLEVBQUUsTUFBeUI7SUFDaEYsSUFBSSxNQUFNLElBQUksTUFBTSxDQUFDLE9BQU8sRUFBRTtRQUM1QixPQUFPLE1BQU0sQ0FBQyxPQUFPLENBQUMsV0FBVyxFQUFFLENBQUMsT0FBTyxDQUFDLFdBQVcsQ0FBQyxXQUFXLEVBQUUsQ0FBQyxHQUFHLENBQUMsQ0FBQyxDQUFDO0tBQzdFO1NBQU07UUFDTCxPQUFPLEtBQUssQ0FBQztLQUNkO0FBQ0gsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7IFBpcGUsIFBpcGVUcmFuc2Zvcm0gfSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7IE56T3B0aW9uR3JvdXBDb21wb25lbnQgfSBmcm9tICcuL256LW9wdGlvbi1ncm91cC5jb21wb25lbnQnO1xuaW1wb3J0IHsgTnpPcHRpb25Db21wb25lbnQgfSBmcm9tICcuL256LW9wdGlvbi5jb21wb25lbnQnO1xuXG5leHBvcnQgdHlwZSBURmlsdGVyT3B0aW9uID0gKGlucHV0OiBzdHJpbmcsIG9wdGlvbjogTnpPcHRpb25Db21wb25lbnQpID0+IGJvb2xlYW47XG5cbkBQaXBlKHsgbmFtZTogJ256RmlsdGVyT3B0aW9uJyB9KVxuZXhwb3J0IGNsYXNzIE56RmlsdGVyT3B0aW9uUGlwZSBpbXBsZW1lbnRzIFBpcGVUcmFuc2Zvcm0ge1xuICB0cmFuc2Zvcm0oXG4gICAgb3B0aW9uczogTnpPcHRpb25Db21wb25lbnRbXSxcbiAgICBzZWFyY2hWYWx1ZTogc3RyaW5nLFxuICAgIGZpbHRlck9wdGlvbjogVEZpbHRlck9wdGlvbixcbiAgICBzZXJ2ZXJTZWFyY2g6IGJvb2xlYW5cbiAgKTogTnpPcHRpb25Db21wb25lbnRbXSB7XG4gICAgaWYgKHNlcnZlclNlYXJjaCB8fCAhc2VhcmNoVmFsdWUpIHtcbiAgICAgIHJldHVybiBvcHRpb25zO1xuICAgIH0gZWxzZSB7XG4gICAgICByZXR1cm4gKG9wdGlvbnMgYXMgTnpPcHRpb25Db21wb25lbnRbXSkuZmlsdGVyKG8gPT4gZmlsdGVyT3B0aW9uKHNlYXJjaFZhbHVlLCBvKSk7XG4gICAgfVxuICB9XG59XG5cbkBQaXBlKHsgbmFtZTogJ256RmlsdGVyR3JvdXBPcHRpb24nIH0pXG5leHBvcnQgY2xhc3MgTnpGaWx0ZXJHcm91cE9wdGlvblBpcGUgaW1wbGVtZW50cyBQaXBlVHJhbnNmb3JtIHtcbiAgdHJhbnNmb3JtKFxuICAgIGdyb3VwczogTnpPcHRpb25Hcm91cENvbXBvbmVudFtdLFxuICAgIHNlYXJjaFZhbHVlOiBzdHJpbmcsXG4gICAgZmlsdGVyT3B0aW9uOiBURmlsdGVyT3B0aW9uLFxuICAgIHNlcnZlclNlYXJjaDogYm9vbGVhblxuICApOiBOek9wdGlvbkdyb3VwQ29tcG9uZW50W10ge1xuICAgIGlmIChzZXJ2ZXJTZWFyY2ggfHwgIXNlYXJjaFZhbHVlKSB7XG4gICAgICByZXR1cm4gZ3JvdXBzO1xuICAgIH0gZWxzZSB7XG4gICAgICByZXR1cm4gKGdyb3VwcyBhcyBOek9wdGlvbkdyb3VwQ29tcG9uZW50W10pLmZpbHRlcihnID0+IHtcbiAgICAgICAgcmV0dXJuIGcubGlzdE9mTnpPcHRpb25Db21wb25lbnQuc29tZShvID0+IGZpbHRlck9wdGlvbihzZWFyY2hWYWx1ZSwgbykpO1xuICAgICAgfSk7XG4gICAgfVxuICB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBkZWZhdWx0RmlsdGVyT3B0aW9uKHNlYXJjaFZhbHVlOiBzdHJpbmcsIG9wdGlvbjogTnpPcHRpb25Db21wb25lbnQpOiBib29sZWFuIHtcbiAgaWYgKG9wdGlvbiAmJiBvcHRpb24ubnpMYWJlbCkge1xuICAgIHJldHVybiBvcHRpb24ubnpMYWJlbC50b0xvd2VyQ2FzZSgpLmluZGV4T2Yoc2VhcmNoVmFsdWUudG9Mb3dlckNhc2UoKSkgPiAtMTtcbiAgfSBlbHNlIHtcbiAgICByZXR1cm4gZmFsc2U7XG4gIH1cbn1cbiJdfQ==