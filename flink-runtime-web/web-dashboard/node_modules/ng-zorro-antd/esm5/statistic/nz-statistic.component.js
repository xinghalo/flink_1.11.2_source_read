/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { ChangeDetectionStrategy, Component, Input, TemplateRef, ViewEncapsulation } from '@angular/core';
var NzStatisticComponent = /** @class */ (function () {
    function NzStatisticComponent() {
        this.nzValueStyle = {};
    }
    NzStatisticComponent.decorators = [
        { type: Component, args: [{
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    encapsulation: ViewEncapsulation.None,
                    selector: 'nz-statistic',
                    template: "<div class=\"ant-statistic-title\">\n  <ng-container *nzStringTemplateOutlet=\"nzTitle\">{{ nzTitle }}</ng-container>\n</div>\n<div class=\"ant-statistic-content\" [ngStyle]=\"nzValueStyle\">\n  <span *ngIf=\"nzPrefix\" class=\"ant-statistic-content-prefix\">\n    <ng-container *nzStringTemplateOutlet=\"nzPrefix\">{{ nzPrefix }}</ng-container>\n  </span>\n  <nz-statistic-number\n    [nzValue]=\"nzValue\"\n    [nzValueTemplate]=\"nzValueTemplate\">\n  </nz-statistic-number>\n  <span *ngIf=\"nzSuffix\" class=\"ant-statistic-content-suffix\">\n    <ng-container *nzStringTemplateOutlet=\"nzSuffix\">{{ nzSuffix }}</ng-container>\n  </span>\n</div>\n",
                    host: {
                        class: 'ant-statistic'
                    },
                    styles: ['nz-statistic { display: block; }']
                }] }
    ];
    NzStatisticComponent.propDecorators = {
        nzPrefix: [{ type: Input }],
        nzSuffix: [{ type: Input }],
        nzTitle: [{ type: Input }],
        nzValue: [{ type: Input }],
        nzValueStyle: [{ type: Input }],
        nzValueTemplate: [{ type: Input }]
    };
    return NzStatisticComponent;
}());
export { NzStatisticComponent };
if (false) {
    /** @type {?} */
    NzStatisticComponent.prototype.nzPrefix;
    /** @type {?} */
    NzStatisticComponent.prototype.nzSuffix;
    /** @type {?} */
    NzStatisticComponent.prototype.nzTitle;
    /** @type {?} */
    NzStatisticComponent.prototype.nzValue;
    /** @type {?} */
    NzStatisticComponent.prototype.nzValueStyle;
    /** @type {?} */
    NzStatisticComponent.prototype.nzValueTemplate;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotc3RhdGlzdGljLmNvbXBvbmVudC5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJzdGF0aXN0aWMvbnotc3RhdGlzdGljLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUFFLHVCQUF1QixFQUFFLFNBQVMsRUFBRSxLQUFLLEVBQUUsV0FBVyxFQUFFLGlCQUFpQixFQUFFLE1BQU0sZUFBZSxDQUFDO0FBRzFHO0lBQUE7UUFlVyxpQkFBWSxHQUFHLEVBQUUsQ0FBQztJQUU3QixDQUFDOztnQkFqQkEsU0FBUyxTQUFDO29CQUNULGVBQWUsRUFBRSx1QkFBdUIsQ0FBQyxNQUFNO29CQUMvQyxhQUFhLEVBQUUsaUJBQWlCLENBQUMsSUFBSTtvQkFDckMsUUFBUSxFQUFFLGNBQWM7b0JBQ3hCLHdwQkFBNEM7b0JBQzVDLElBQUksRUFBRTt3QkFDSixLQUFLLEVBQUUsZUFBZTtxQkFDdkI7NkJBQ1Esa0NBQWtDO2lCQUM1Qzs7OzJCQUVFLEtBQUs7MkJBQ0wsS0FBSzswQkFDTCxLQUFLOzBCQUNMLEtBQUs7K0JBQ0wsS0FBSztrQ0FDTCxLQUFLOztJQUNSLDJCQUFDO0NBQUEsQUFqQkQsSUFpQkM7U0FQWSxvQkFBb0I7OztJQUMvQix3Q0FBOEM7O0lBQzlDLHdDQUE4Qzs7SUFDOUMsdUNBQTZDOztJQUM3Qyx1Q0FBdUM7O0lBQ3ZDLDRDQUEyQjs7SUFDM0IsK0NBQTJFIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3ksIENvbXBvbmVudCwgSW5wdXQsIFRlbXBsYXRlUmVmLCBWaWV3RW5jYXBzdWxhdGlvbiB9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuaW1wb3J0IHsgTnpTdGF0aXN0aWNWYWx1ZVR5cGUgfSBmcm9tICcuL256LXN0YXRpc3RpYy1kZWZpbml0aW9ucyc7XG5cbkBDb21wb25lbnQoe1xuICBjaGFuZ2VEZXRlY3Rpb246IENoYW5nZURldGVjdGlvblN0cmF0ZWd5Lk9uUHVzaCxcbiAgZW5jYXBzdWxhdGlvbjogVmlld0VuY2Fwc3VsYXRpb24uTm9uZSxcbiAgc2VsZWN0b3I6ICduei1zdGF0aXN0aWMnLFxuICB0ZW1wbGF0ZVVybDogJy4vbnotc3RhdGlzdGljLmNvbXBvbmVudC5odG1sJyxcbiAgaG9zdDoge1xuICAgIGNsYXNzOiAnYW50LXN0YXRpc3RpYydcbiAgfSxcbiAgc3R5bGVzOiBbJ256LXN0YXRpc3RpYyB7IGRpc3BsYXk6IGJsb2NrOyB9J11cbn0pXG5leHBvcnQgY2xhc3MgTnpTdGF0aXN0aWNDb21wb25lbnQge1xuICBASW5wdXQoKSBuelByZWZpeDogc3RyaW5nIHwgVGVtcGxhdGVSZWY8dm9pZD47XG4gIEBJbnB1dCgpIG56U3VmZml4OiBzdHJpbmcgfCBUZW1wbGF0ZVJlZjx2b2lkPjtcbiAgQElucHV0KCkgbnpUaXRsZTogc3RyaW5nIHwgVGVtcGxhdGVSZWY8dm9pZD47XG4gIEBJbnB1dCgpIG56VmFsdWU6IE56U3RhdGlzdGljVmFsdWVUeXBlO1xuICBASW5wdXQoKSBuelZhbHVlU3R5bGUgPSB7fTtcbiAgQElucHV0KCkgbnpWYWx1ZVRlbXBsYXRlOiBUZW1wbGF0ZVJlZjx7ICRpbXBsaWNpdDogTnpTdGF0aXN0aWNWYWx1ZVR5cGUgfT47XG59XG4iXX0=