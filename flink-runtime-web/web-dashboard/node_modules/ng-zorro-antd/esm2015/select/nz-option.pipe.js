/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { Pipe } from '@angular/core';
export class NzFilterOptionPipe {
    /**
     * @param {?} options
     * @param {?} searchValue
     * @param {?} filterOption
     * @param {?} serverSearch
     * @return {?}
     */
    transform(options, searchValue, filterOption, serverSearch) {
        if (serverSearch || !searchValue) {
            return options;
        }
        else {
            return ((/** @type {?} */ (options))).filter((/**
             * @param {?} o
             * @return {?}
             */
            o => filterOption(searchValue, o)));
        }
    }
}
NzFilterOptionPipe.decorators = [
    { type: Pipe, args: [{ name: 'nzFilterOption' },] }
];
export class NzFilterGroupOptionPipe {
    /**
     * @param {?} groups
     * @param {?} searchValue
     * @param {?} filterOption
     * @param {?} serverSearch
     * @return {?}
     */
    transform(groups, searchValue, filterOption, serverSearch) {
        if (serverSearch || !searchValue) {
            return groups;
        }
        else {
            return ((/** @type {?} */ (groups))).filter((/**
             * @param {?} g
             * @return {?}
             */
            g => {
                return g.listOfNzOptionComponent.some((/**
                 * @param {?} o
                 * @return {?}
                 */
                o => filterOption(searchValue, o)));
            }));
        }
    }
}
NzFilterGroupOptionPipe.decorators = [
    { type: Pipe, args: [{ name: 'nzFilterGroupOption' },] }
];
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotb3B0aW9uLnBpcGUuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsic2VsZWN0L256LW9wdGlvbi5waXBlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7QUFBQSxPQUFPLEVBQUUsSUFBSSxFQUFpQixNQUFNLGVBQWUsQ0FBQztBQU9wRCxNQUFNLE9BQU8sa0JBQWtCOzs7Ozs7OztJQUM3QixTQUFTLENBQ1AsT0FBNEIsRUFDNUIsV0FBbUIsRUFDbkIsWUFBMkIsRUFDM0IsWUFBcUI7UUFFckIsSUFBSSxZQUFZLElBQUksQ0FBQyxXQUFXLEVBQUU7WUFDaEMsT0FBTyxPQUFPLENBQUM7U0FDaEI7YUFBTTtZQUNMLE9BQU8sQ0FBQyxtQkFBQSxPQUFPLEVBQXVCLENBQUMsQ0FBQyxNQUFNOzs7O1lBQUMsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxZQUFZLENBQUMsV0FBVyxFQUFFLENBQUMsQ0FBQyxFQUFDLENBQUM7U0FDbkY7SUFDSCxDQUFDOzs7WUFiRixJQUFJLFNBQUMsRUFBRSxJQUFJLEVBQUUsZ0JBQWdCLEVBQUU7O0FBaUJoQyxNQUFNLE9BQU8sdUJBQXVCOzs7Ozs7OztJQUNsQyxTQUFTLENBQ1AsTUFBZ0MsRUFDaEMsV0FBbUIsRUFDbkIsWUFBMkIsRUFDM0IsWUFBcUI7UUFFckIsSUFBSSxZQUFZLElBQUksQ0FBQyxXQUFXLEVBQUU7WUFDaEMsT0FBTyxNQUFNLENBQUM7U0FDZjthQUFNO1lBQ0wsT0FBTyxDQUFDLG1CQUFBLE1BQU0sRUFBNEIsQ0FBQyxDQUFDLE1BQU07Ozs7WUFBQyxDQUFDLENBQUMsRUFBRTtnQkFDckQsT0FBTyxDQUFDLENBQUMsdUJBQXVCLENBQUMsSUFBSTs7OztnQkFBQyxDQUFDLENBQUMsRUFBRSxDQUFDLFlBQVksQ0FBQyxXQUFXLEVBQUUsQ0FBQyxDQUFDLEVBQUMsQ0FBQztZQUMzRSxDQUFDLEVBQUMsQ0FBQztTQUNKO0lBQ0gsQ0FBQzs7O1lBZkYsSUFBSSxTQUFDLEVBQUUsSUFBSSxFQUFFLHFCQUFxQixFQUFFOzs7Ozs7O0FBa0JyQyxNQUFNLFVBQVUsbUJBQW1CLENBQUMsV0FBbUIsRUFBRSxNQUF5QjtJQUNoRixJQUFJLE1BQU0sSUFBSSxNQUFNLENBQUMsT0FBTyxFQUFFO1FBQzVCLE9BQU8sTUFBTSxDQUFDLE9BQU8sQ0FBQyxXQUFXLEVBQUUsQ0FBQyxPQUFPLENBQUMsV0FBVyxDQUFDLFdBQVcsRUFBRSxDQUFDLEdBQUcsQ0FBQyxDQUFDLENBQUM7S0FDN0U7U0FBTTtRQUNMLE9BQU8sS0FBSyxDQUFDO0tBQ2Q7QUFDSCxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgUGlwZSwgUGlwZVRyYW5zZm9ybSB9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuaW1wb3J0IHsgTnpPcHRpb25Hcm91cENvbXBvbmVudCB9IGZyb20gJy4vbnotb3B0aW9uLWdyb3VwLmNvbXBvbmVudCc7XG5pbXBvcnQgeyBOek9wdGlvbkNvbXBvbmVudCB9IGZyb20gJy4vbnotb3B0aW9uLmNvbXBvbmVudCc7XG5cbmV4cG9ydCB0eXBlIFRGaWx0ZXJPcHRpb24gPSAoaW5wdXQ6IHN0cmluZywgb3B0aW9uOiBOek9wdGlvbkNvbXBvbmVudCkgPT4gYm9vbGVhbjtcblxuQFBpcGUoeyBuYW1lOiAnbnpGaWx0ZXJPcHRpb24nIH0pXG5leHBvcnQgY2xhc3MgTnpGaWx0ZXJPcHRpb25QaXBlIGltcGxlbWVudHMgUGlwZVRyYW5zZm9ybSB7XG4gIHRyYW5zZm9ybShcbiAgICBvcHRpb25zOiBOek9wdGlvbkNvbXBvbmVudFtdLFxuICAgIHNlYXJjaFZhbHVlOiBzdHJpbmcsXG4gICAgZmlsdGVyT3B0aW9uOiBURmlsdGVyT3B0aW9uLFxuICAgIHNlcnZlclNlYXJjaDogYm9vbGVhblxuICApOiBOek9wdGlvbkNvbXBvbmVudFtdIHtcbiAgICBpZiAoc2VydmVyU2VhcmNoIHx8ICFzZWFyY2hWYWx1ZSkge1xuICAgICAgcmV0dXJuIG9wdGlvbnM7XG4gICAgfSBlbHNlIHtcbiAgICAgIHJldHVybiAob3B0aW9ucyBhcyBOek9wdGlvbkNvbXBvbmVudFtdKS5maWx0ZXIobyA9PiBmaWx0ZXJPcHRpb24oc2VhcmNoVmFsdWUsIG8pKTtcbiAgICB9XG4gIH1cbn1cblxuQFBpcGUoeyBuYW1lOiAnbnpGaWx0ZXJHcm91cE9wdGlvbicgfSlcbmV4cG9ydCBjbGFzcyBOekZpbHRlckdyb3VwT3B0aW9uUGlwZSBpbXBsZW1lbnRzIFBpcGVUcmFuc2Zvcm0ge1xuICB0cmFuc2Zvcm0oXG4gICAgZ3JvdXBzOiBOek9wdGlvbkdyb3VwQ29tcG9uZW50W10sXG4gICAgc2VhcmNoVmFsdWU6IHN0cmluZyxcbiAgICBmaWx0ZXJPcHRpb246IFRGaWx0ZXJPcHRpb24sXG4gICAgc2VydmVyU2VhcmNoOiBib29sZWFuXG4gICk6IE56T3B0aW9uR3JvdXBDb21wb25lbnRbXSB7XG4gICAgaWYgKHNlcnZlclNlYXJjaCB8fCAhc2VhcmNoVmFsdWUpIHtcbiAgICAgIHJldHVybiBncm91cHM7XG4gICAgfSBlbHNlIHtcbiAgICAgIHJldHVybiAoZ3JvdXBzIGFzIE56T3B0aW9uR3JvdXBDb21wb25lbnRbXSkuZmlsdGVyKGcgPT4ge1xuICAgICAgICByZXR1cm4gZy5saXN0T2ZOek9wdGlvbkNvbXBvbmVudC5zb21lKG8gPT4gZmlsdGVyT3B0aW9uKHNlYXJjaFZhbHVlLCBvKSk7XG4gICAgICB9KTtcbiAgICB9XG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGRlZmF1bHRGaWx0ZXJPcHRpb24oc2VhcmNoVmFsdWU6IHN0cmluZywgb3B0aW9uOiBOek9wdGlvbkNvbXBvbmVudCk6IGJvb2xlYW4ge1xuICBpZiAob3B0aW9uICYmIG9wdGlvbi5uekxhYmVsKSB7XG4gICAgcmV0dXJuIG9wdGlvbi5uekxhYmVsLnRvTG93ZXJDYXNlKCkuaW5kZXhPZihzZWFyY2hWYWx1ZS50b0xvd2VyQ2FzZSgpKSA+IC0xO1xuICB9IGVsc2Uge1xuICAgIHJldHVybiBmYWxzZTtcbiAgfVxufVxuIl19