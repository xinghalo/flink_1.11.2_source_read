/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { ChangeDetectionStrategy, Component, Input, TemplateRef, ViewChild, ViewEncapsulation } from '@angular/core';
import { InputBoolean } from '../core/util/convert';
export class NzOptionComponent {
    constructor() {
        this.nzDisabled = false;
        this.nzCustomContent = false;
    }
}
NzOptionComponent.decorators = [
    { type: Component, args: [{
                selector: 'nz-option',
                encapsulation: ViewEncapsulation.None,
                changeDetection: ChangeDetectionStrategy.OnPush,
                template: "<ng-template>\n  <ng-content></ng-content>\n</ng-template>"
            }] }
];
NzOptionComponent.propDecorators = {
    template: [{ type: ViewChild, args: [TemplateRef,] }],
    nzLabel: [{ type: Input }],
    nzValue: [{ type: Input }],
    nzDisabled: [{ type: Input }],
    nzCustomContent: [{ type: Input }]
};
tslib_1.__decorate([
    InputBoolean(),
    tslib_1.__metadata("design:type", Object)
], NzOptionComponent.prototype, "nzDisabled", void 0);
tslib_1.__decorate([
    InputBoolean(),
    tslib_1.__metadata("design:type", Object)
], NzOptionComponent.prototype, "nzCustomContent", void 0);
if (false) {
    /** @type {?} */
    NzOptionComponent.prototype.template;
    /** @type {?} */
    NzOptionComponent.prototype.nzLabel;
    /** @type {?} */
    NzOptionComponent.prototype.nzValue;
    /** @type {?} */
    NzOptionComponent.prototype.nzDisabled;
    /** @type {?} */
    NzOptionComponent.prototype.nzCustomContent;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotb3B0aW9uLmNvbXBvbmVudC5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJzZWxlY3Qvbnotb3B0aW9uLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFBRSx1QkFBdUIsRUFBRSxTQUFTLEVBQUUsS0FBSyxFQUFFLFdBQVcsRUFBRSxTQUFTLEVBQUUsaUJBQWlCLEVBQUUsTUFBTSxlQUFlLENBQUM7QUFFckgsT0FBTyxFQUFFLFlBQVksRUFBRSxNQUFNLHNCQUFzQixDQUFDO0FBUXBELE1BQU0sT0FBTyxpQkFBaUI7SUFOOUI7UUFXMkIsZUFBVSxHQUFHLEtBQUssQ0FBQztRQUNuQixvQkFBZSxHQUFHLEtBQUssQ0FBQztJQUNuRCxDQUFDOzs7WUFiQSxTQUFTLFNBQUM7Z0JBQ1QsUUFBUSxFQUFFLFdBQVc7Z0JBQ3JCLGFBQWEsRUFBRSxpQkFBaUIsQ0FBQyxJQUFJO2dCQUNyQyxlQUFlLEVBQUUsdUJBQXVCLENBQUMsTUFBTTtnQkFDL0Msc0VBQXlDO2FBQzFDOzs7dUJBRUUsU0FBUyxTQUFDLFdBQVc7c0JBQ3JCLEtBQUs7c0JBRUwsS0FBSzt5QkFDTCxLQUFLOzhCQUNMLEtBQUs7O0FBRG1CO0lBQWYsWUFBWSxFQUFFOztxREFBb0I7QUFDbkI7SUFBZixZQUFZLEVBQUU7OzBEQUF5Qjs7O0lBTGpELHFDQUFvRDs7SUFDcEQsb0NBQXlCOztJQUV6QixvQ0FBc0I7O0lBQ3RCLHVDQUE0Qzs7SUFDNUMsNENBQWlEIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3ksIENvbXBvbmVudCwgSW5wdXQsIFRlbXBsYXRlUmVmLCBWaWV3Q2hpbGQsIFZpZXdFbmNhcHN1bGF0aW9uIH0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5cbmltcG9ydCB7IElucHV0Qm9vbGVhbiB9IGZyb20gJy4uL2NvcmUvdXRpbC9jb252ZXJ0JztcblxuQENvbXBvbmVudCh7XG4gIHNlbGVjdG9yOiAnbnotb3B0aW9uJyxcbiAgZW5jYXBzdWxhdGlvbjogVmlld0VuY2Fwc3VsYXRpb24uTm9uZSxcbiAgY2hhbmdlRGV0ZWN0aW9uOiBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneS5PblB1c2gsXG4gIHRlbXBsYXRlVXJsOiAnLi9uei1vcHRpb24uY29tcG9uZW50Lmh0bWwnXG59KVxuZXhwb3J0IGNsYXNzIE56T3B0aW9uQ29tcG9uZW50IHtcbiAgQFZpZXdDaGlsZChUZW1wbGF0ZVJlZikgdGVtcGxhdGU6IFRlbXBsYXRlUmVmPHZvaWQ+O1xuICBASW5wdXQoKSBuekxhYmVsOiBzdHJpbmc7XG4gIC8vIHRzbGludDpkaXNhYmxlLW5leHQtbGluZTpuby1hbnlcbiAgQElucHV0KCkgbnpWYWx1ZTogYW55O1xuICBASW5wdXQoKSBASW5wdXRCb29sZWFuKCkgbnpEaXNhYmxlZCA9IGZhbHNlO1xuICBASW5wdXQoKSBASW5wdXRCb29sZWFuKCkgbnpDdXN0b21Db250ZW50ID0gZmFsc2U7XG59XG4iXX0=