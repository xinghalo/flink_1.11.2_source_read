/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { Directive, ElementRef, Input, Renderer2 } from '@angular/core';
export class NzClassListAddDirective {
    /**
     * @param {?} elementRef
     * @param {?} renderer
     */
    constructor(elementRef, renderer) {
        this.elementRef = elementRef;
        this.renderer = renderer;
        this.classList = [];
    }
    /**
     * @param {?} list
     * @return {?}
     */
    set nzClassListAdd(list) {
        this.classList.forEach((/**
         * @param {?} name
         * @return {?}
         */
        name => {
            this.renderer.removeClass(this.elementRef.nativeElement, name);
        }));
        list.forEach((/**
         * @param {?} name
         * @return {?}
         */
        name => {
            this.renderer.addClass(this.elementRef.nativeElement, name);
        }));
        this.classList = list;
    }
}
NzClassListAddDirective.decorators = [
    { type: Directive, args: [{
                selector: '[nzClassListAdd]'
            },] }
];
/** @nocollapse */
NzClassListAddDirective.ctorParameters = () => [
    { type: ElementRef },
    { type: Renderer2 }
];
NzClassListAddDirective.propDecorators = {
    nzClassListAdd: [{ type: Input }]
};
if (false) {
    /** @type {?} */
    NzClassListAddDirective.prototype.classList;
    /**
     * @type {?}
     * @private
     */
    NzClassListAddDirective.prototype.elementRef;
    /**
     * @type {?}
     * @private
     */
    NzClassListAddDirective.prototype.renderer;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY2xhc3NsaXN0X2FkZC5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJjb3JlL2FkZG9uL2NsYXNzbGlzdF9hZGQudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7OztBQUFBLE9BQU8sRUFBRSxTQUFTLEVBQUUsVUFBVSxFQUFFLEtBQUssRUFBRSxTQUFTLEVBQUUsTUFBTSxlQUFlLENBQUM7QUFLeEUsTUFBTSxPQUFPLHVCQUF1Qjs7Ozs7SUFjbEMsWUFBb0IsVUFBc0IsRUFBVSxRQUFtQjtRQUFuRCxlQUFVLEdBQVYsVUFBVSxDQUFZO1FBQVUsYUFBUSxHQUFSLFFBQVEsQ0FBVztRQWJ2RSxjQUFTLEdBQWEsRUFBRSxDQUFDO0lBY3pCLENBQUM7Ozs7O0lBWkQsSUFDSSxjQUFjLENBQUMsSUFBYztRQUMvQixJQUFJLENBQUMsU0FBUyxDQUFDLE9BQU87Ozs7UUFBQyxJQUFJLENBQUMsRUFBRTtZQUM1QixJQUFJLENBQUMsUUFBUSxDQUFDLFdBQVcsQ0FBQyxJQUFJLENBQUMsVUFBVSxDQUFDLGFBQWEsRUFBRSxJQUFJLENBQUMsQ0FBQztRQUNqRSxDQUFDLEVBQUMsQ0FBQztRQUNILElBQUksQ0FBQyxPQUFPOzs7O1FBQUMsSUFBSSxDQUFDLEVBQUU7WUFDbEIsSUFBSSxDQUFDLFFBQVEsQ0FBQyxRQUFRLENBQUMsSUFBSSxDQUFDLFVBQVUsQ0FBQyxhQUFhLEVBQUUsSUFBSSxDQUFDLENBQUM7UUFDOUQsQ0FBQyxFQUFDLENBQUM7UUFDSCxJQUFJLENBQUMsU0FBUyxHQUFHLElBQUksQ0FBQztJQUN4QixDQUFDOzs7WUFmRixTQUFTLFNBQUM7Z0JBQ1QsUUFBUSxFQUFFLGtCQUFrQjthQUM3Qjs7OztZQUptQixVQUFVO1lBQVMsU0FBUzs7OzZCQVE3QyxLQUFLOzs7O0lBRk4sNENBQXlCOzs7OztJQWFiLDZDQUE4Qjs7Ozs7SUFBRSwyQ0FBMkIiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgeyBEaXJlY3RpdmUsIEVsZW1lbnRSZWYsIElucHV0LCBSZW5kZXJlcjIgfSBmcm9tICdAYW5ndWxhci9jb3JlJztcblxuQERpcmVjdGl2ZSh7XG4gIHNlbGVjdG9yOiAnW256Q2xhc3NMaXN0QWRkXSdcbn0pXG5leHBvcnQgY2xhc3MgTnpDbGFzc0xpc3RBZGREaXJlY3RpdmUge1xuICBjbGFzc0xpc3Q6IHN0cmluZ1tdID0gW107XG5cbiAgQElucHV0KClcbiAgc2V0IG56Q2xhc3NMaXN0QWRkKGxpc3Q6IHN0cmluZ1tdKSB7XG4gICAgdGhpcy5jbGFzc0xpc3QuZm9yRWFjaChuYW1lID0+IHtcbiAgICAgIHRoaXMucmVuZGVyZXIucmVtb3ZlQ2xhc3ModGhpcy5lbGVtZW50UmVmLm5hdGl2ZUVsZW1lbnQsIG5hbWUpO1xuICAgIH0pO1xuICAgIGxpc3QuZm9yRWFjaChuYW1lID0+IHtcbiAgICAgIHRoaXMucmVuZGVyZXIuYWRkQ2xhc3ModGhpcy5lbGVtZW50UmVmLm5hdGl2ZUVsZW1lbnQsIG5hbWUpO1xuICAgIH0pO1xuICAgIHRoaXMuY2xhc3NMaXN0ID0gbGlzdDtcbiAgfVxuXG4gIGNvbnN0cnVjdG9yKHByaXZhdGUgZWxlbWVudFJlZjogRWxlbWVudFJlZiwgcHJpdmF0ZSByZW5kZXJlcjogUmVuZGVyZXIyKSB7XG4gIH1cbn1cbiJdfQ==