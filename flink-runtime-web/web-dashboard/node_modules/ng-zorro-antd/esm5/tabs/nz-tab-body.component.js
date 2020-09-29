/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { ChangeDetectionStrategy, Component, Input, TemplateRef, ViewEncapsulation } from '@angular/core';
var NzTabBodyComponent = /** @class */ (function () {
    function NzTabBodyComponent() {
        this.active = false;
        this.forceRender = false;
    }
    NzTabBodyComponent.decorators = [
        { type: Component, args: [{
                    selector: '[nz-tab-body]',
                    preserveWhitespaces: false,
                    encapsulation: ViewEncapsulation.None,
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    template: "<ng-container *ngIf=\"active || forceRender\">\n  <ng-template [ngTemplateOutlet]=\"content\"></ng-template>\n</ng-container>",
                    host: {
                        '[class.ant-tabs-tabpane-active]': 'active',
                        '[class.ant-tabs-tabpane-inactive]': '!active'
                    }
                }] }
    ];
    NzTabBodyComponent.propDecorators = {
        content: [{ type: Input }],
        active: [{ type: Input }],
        forceRender: [{ type: Input }]
    };
    return NzTabBodyComponent;
}());
export { NzTabBodyComponent };
if (false) {
    /** @type {?} */
    NzTabBodyComponent.prototype.content;
    /** @type {?} */
    NzTabBodyComponent.prototype.active;
    /** @type {?} */
    NzTabBodyComponent.prototype.forceRender;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotdGFiLWJvZHkuY29tcG9uZW50LmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbInRhYnMvbnotdGFiLWJvZHkuY29tcG9uZW50LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7QUFBQSxPQUFPLEVBQUUsdUJBQXVCLEVBQUUsU0FBUyxFQUFFLEtBQUssRUFBRSxXQUFXLEVBQUUsaUJBQWlCLEVBQUUsTUFBTSxlQUFlLENBQUM7QUFFMUc7SUFBQTtRQWFXLFdBQU0sR0FBRyxLQUFLLENBQUM7UUFDZixnQkFBVyxHQUFHLEtBQUssQ0FBQztJQUMvQixDQUFDOztnQkFmQSxTQUFTLFNBQUM7b0JBQ1QsUUFBUSxFQUFFLGVBQWU7b0JBQ3pCLG1CQUFtQixFQUFFLEtBQUs7b0JBQzFCLGFBQWEsRUFBRSxpQkFBaUIsQ0FBQyxJQUFJO29CQUNyQyxlQUFlLEVBQUUsdUJBQXVCLENBQUMsTUFBTTtvQkFDL0MseUlBQTJDO29CQUMzQyxJQUFJLEVBQUU7d0JBQ0osaUNBQWlDLEVBQUUsUUFBUTt3QkFDM0MsbUNBQW1DLEVBQUUsU0FBUztxQkFDL0M7aUJBQ0Y7OzswQkFFRSxLQUFLO3lCQUNMLEtBQUs7OEJBQ0wsS0FBSzs7SUFDUix5QkFBQztDQUFBLEFBZkQsSUFlQztTQUpZLGtCQUFrQjs7O0lBQzdCLHFDQUFvQzs7SUFDcEMsb0NBQXdCOztJQUN4Qix5Q0FBNkIiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgeyBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneSwgQ29tcG9uZW50LCBJbnB1dCwgVGVtcGxhdGVSZWYsIFZpZXdFbmNhcHN1bGF0aW9uIH0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5cbkBDb21wb25lbnQoe1xuICBzZWxlY3RvcjogJ1tuei10YWItYm9keV0nLFxuICBwcmVzZXJ2ZVdoaXRlc3BhY2VzOiBmYWxzZSxcbiAgZW5jYXBzdWxhdGlvbjogVmlld0VuY2Fwc3VsYXRpb24uTm9uZSxcbiAgY2hhbmdlRGV0ZWN0aW9uOiBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneS5PblB1c2gsXG4gIHRlbXBsYXRlVXJsOiAnLi9uei10YWItYm9keS5jb21wb25lbnQuaHRtbCcsXG4gIGhvc3Q6IHtcbiAgICAnW2NsYXNzLmFudC10YWJzLXRhYnBhbmUtYWN0aXZlXSc6ICdhY3RpdmUnLFxuICAgICdbY2xhc3MuYW50LXRhYnMtdGFicGFuZS1pbmFjdGl2ZV0nOiAnIWFjdGl2ZSdcbiAgfVxufSlcbmV4cG9ydCBjbGFzcyBOelRhYkJvZHlDb21wb25lbnQge1xuICBASW5wdXQoKSBjb250ZW50OiBUZW1wbGF0ZVJlZjx2b2lkPjtcbiAgQElucHV0KCkgYWN0aXZlID0gZmFsc2U7XG4gIEBJbnB1dCgpIGZvcmNlUmVuZGVyID0gZmFsc2U7XG59XG4iXX0=