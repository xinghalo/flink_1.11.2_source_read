/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { ChangeDetectionStrategy, Component, ViewEncapsulation } from '@angular/core';
import { NzBreadCrumbComponent } from './nz-breadcrumb.component';
var NzBreadCrumbItemComponent = /** @class */ (function () {
    function NzBreadCrumbItemComponent(nzBreadCrumbComponent) {
        this.nzBreadCrumbComponent = nzBreadCrumbComponent;
    }
    NzBreadCrumbItemComponent.decorators = [
        { type: Component, args: [{
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    encapsulation: ViewEncapsulation.None,
                    selector: 'nz-breadcrumb-item',
                    preserveWhitespaces: false,
                    template: "<span class=\"ant-breadcrumb-link\">\n  <ng-content></ng-content>\n</span>\n<span class=\"ant-breadcrumb-separator\">\n  <ng-container *nzStringTemplateOutlet=\"nzBreadCrumbComponent.nzSeparator\">\n    {{ nzBreadCrumbComponent.nzSeparator }}\n  </ng-container>\n</span>",
                    styles: ["\n      nz-breadcrumb-item:last-child {\n        color: rgba(0, 0, 0, 0.65);\n      }\n\n      nz-breadcrumb-item:last-child .ant-breadcrumb-separator {\n        display: none;\n      }\n    "]
                }] }
    ];
    /** @nocollapse */
    NzBreadCrumbItemComponent.ctorParameters = function () { return [
        { type: NzBreadCrumbComponent }
    ]; };
    return NzBreadCrumbItemComponent;
}());
export { NzBreadCrumbItemComponent };
if (false) {
    /** @type {?} */
    NzBreadCrumbItemComponent.prototype.nzBreadCrumbComponent;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotYnJlYWRjcnVtYi1pdGVtLmNvbXBvbmVudC5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJicmVhZGNydW1iL256LWJyZWFkY3J1bWItaXRlbS5jb21wb25lbnQudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7OztBQUFBLE9BQU8sRUFBRSx1QkFBdUIsRUFBRSxTQUFTLEVBQUUsaUJBQWlCLEVBQUUsTUFBTSxlQUFlLENBQUM7QUFFdEYsT0FBTyxFQUFFLHFCQUFxQixFQUFFLE1BQU0sMkJBQTJCLENBQUM7QUFFbEU7SUFtQkUsbUNBQW1CLHFCQUE0QztRQUE1QywwQkFBcUIsR0FBckIscUJBQXFCLENBQXVCO0lBQUcsQ0FBQzs7Z0JBbkJwRSxTQUFTLFNBQUM7b0JBQ1QsZUFBZSxFQUFFLHVCQUF1QixDQUFDLE1BQU07b0JBQy9DLGFBQWEsRUFBRSxpQkFBaUIsQ0FBQyxJQUFJO29CQUNyQyxRQUFRLEVBQUUsb0JBQW9CO29CQUM5QixtQkFBbUIsRUFBRSxLQUFLO29CQUMxQiwwUkFBZ0Q7NkJBRTlDLGlNQVFDO2lCQUVKOzs7O2dCQW5CUSxxQkFBcUI7O0lBc0I5QixnQ0FBQztDQUFBLEFBcEJELElBb0JDO1NBRlkseUJBQXlCOzs7SUFDeEIsMERBQW1EIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3ksIENvbXBvbmVudCwgVmlld0VuY2Fwc3VsYXRpb24gfSBmcm9tICdAYW5ndWxhci9jb3JlJztcblxuaW1wb3J0IHsgTnpCcmVhZENydW1iQ29tcG9uZW50IH0gZnJvbSAnLi9uei1icmVhZGNydW1iLmNvbXBvbmVudCc7XG5cbkBDb21wb25lbnQoe1xuICBjaGFuZ2VEZXRlY3Rpb246IENoYW5nZURldGVjdGlvblN0cmF0ZWd5Lk9uUHVzaCxcbiAgZW5jYXBzdWxhdGlvbjogVmlld0VuY2Fwc3VsYXRpb24uTm9uZSxcbiAgc2VsZWN0b3I6ICduei1icmVhZGNydW1iLWl0ZW0nLFxuICBwcmVzZXJ2ZVdoaXRlc3BhY2VzOiBmYWxzZSxcbiAgdGVtcGxhdGVVcmw6ICduei1icmVhZGNydW1iLWl0ZW0uY29tcG9uZW50Lmh0bWwnLFxuICBzdHlsZXM6IFtcbiAgICBgXG4gICAgICBuei1icmVhZGNydW1iLWl0ZW06bGFzdC1jaGlsZCB7XG4gICAgICAgIGNvbG9yOiByZ2JhKDAsIDAsIDAsIDAuNjUpO1xuICAgICAgfVxuXG4gICAgICBuei1icmVhZGNydW1iLWl0ZW06bGFzdC1jaGlsZCAuYW50LWJyZWFkY3J1bWItc2VwYXJhdG9yIHtcbiAgICAgICAgZGlzcGxheTogbm9uZTtcbiAgICAgIH1cbiAgICBgXG4gIF1cbn0pXG5leHBvcnQgY2xhc3MgTnpCcmVhZENydW1iSXRlbUNvbXBvbmVudCB7XG4gIGNvbnN0cnVjdG9yKHB1YmxpYyBuekJyZWFkQ3J1bWJDb21wb25lbnQ6IE56QnJlYWRDcnVtYkNvbXBvbmVudCkge31cbn1cbiJdfQ==