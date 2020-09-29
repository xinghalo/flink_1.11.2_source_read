/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { Directive, Host, Optional } from '@angular/core';
import { NzTableComponent } from './nz-table.component';
export class NzTbodyDirective {
    /**
     * @param {?} nzTableComponent
     */
    constructor(nzTableComponent) {
        this.nzTableComponent = nzTableComponent;
    }
}
NzTbodyDirective.decorators = [
    { type: Directive, args: [{
                // tslint:disable-next-line:directive-selector
                selector: 'tbody',
                host: {
                    '[class.ant-table-tbody]': 'nzTableComponent'
                }
            },] }
];
/** @nocollapse */
NzTbodyDirective.ctorParameters = () => [
    { type: NzTableComponent, decorators: [{ type: Host }, { type: Optional }] }
];
if (false) {
    /** @type {?} */
    NzTbodyDirective.prototype.nzTableComponent;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotdGJvZHkuZGlyZWN0aXZlLmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbInRhYmxlL256LXRib2R5LmRpcmVjdGl2ZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUFFLFNBQVMsRUFBRSxJQUFJLEVBQUUsUUFBUSxFQUFFLE1BQU0sZUFBZSxDQUFDO0FBQzFELE9BQU8sRUFBRSxnQkFBZ0IsRUFBRSxNQUFNLHNCQUFzQixDQUFDO0FBU3hELE1BQU0sT0FBTyxnQkFBZ0I7Ozs7SUFDM0IsWUFBdUMsZ0JBQWtDO1FBQWxDLHFCQUFnQixHQUFoQixnQkFBZ0IsQ0FBa0I7SUFBRyxDQUFDOzs7WUFSOUUsU0FBUyxTQUFDOztnQkFFVCxRQUFRLEVBQUUsT0FBTztnQkFDakIsSUFBSSxFQUFFO29CQUNKLHlCQUF5QixFQUFFLGtCQUFrQjtpQkFDOUM7YUFDRjs7OztZQVJRLGdCQUFnQix1QkFVVixJQUFJLFlBQUksUUFBUTs7OztJQUFqQiw0Q0FBNkQiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgeyBEaXJlY3RpdmUsIEhvc3QsIE9wdGlvbmFsIH0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQgeyBOelRhYmxlQ29tcG9uZW50IH0gZnJvbSAnLi9uei10YWJsZS5jb21wb25lbnQnO1xuXG5ARGlyZWN0aXZlKHtcbiAgLy8gdHNsaW50OmRpc2FibGUtbmV4dC1saW5lOmRpcmVjdGl2ZS1zZWxlY3RvclxuICBzZWxlY3RvcjogJ3Rib2R5JyxcbiAgaG9zdDoge1xuICAgICdbY2xhc3MuYW50LXRhYmxlLXRib2R5XSc6ICduelRhYmxlQ29tcG9uZW50J1xuICB9XG59KVxuZXhwb3J0IGNsYXNzIE56VGJvZHlEaXJlY3RpdmUge1xuICBjb25zdHJ1Y3RvcihASG9zdCgpIEBPcHRpb25hbCgpIHB1YmxpYyBuelRhYmxlQ29tcG9uZW50OiBOelRhYmxlQ29tcG9uZW50KSB7fVxufVxuIl19