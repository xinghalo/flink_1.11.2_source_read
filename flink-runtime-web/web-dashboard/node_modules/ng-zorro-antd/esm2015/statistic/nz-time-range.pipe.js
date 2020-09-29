/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { Pipe } from '@angular/core';
import { padStart, timeUnits } from '../core/util';
export class NzTimeRangePipe {
    /**
     * @param {?} value
     * @param {?=} format
     * @return {?}
     */
    transform(value, format = 'HH:mm:ss') {
        /** @type {?} */
        let duration = Number(value || 0);
        return timeUnits.reduce((/**
         * @param {?} current
         * @param {?} __1
         * @return {?}
         */
        (current, [name, unit]) => {
            if (current.indexOf(name) !== -1) {
                /** @type {?} */
                const v = Math.floor(duration / unit);
                duration -= v * unit;
                return current.replace(new RegExp(`${name}+`, 'g'), (/**
                 * @param {?} match
                 * @return {?}
                 */
                (match) => {
                    return padStart(v.toString(), match.length, '0');
                }));
            }
            return current;
        }), format);
    }
}
NzTimeRangePipe.decorators = [
    { type: Pipe, args: [{
                name: 'nzTimeRange',
                pure: true
            },] }
];
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotdGltZS1yYW5nZS5waXBlLmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbInN0YXRpc3RpYy9uei10aW1lLXJhbmdlLnBpcGUudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7OztBQUFBLE9BQU8sRUFBRSxJQUFJLEVBQWlCLE1BQU0sZUFBZSxDQUFDO0FBQ3BELE9BQU8sRUFBRSxRQUFRLEVBQUUsU0FBUyxFQUFFLE1BQU0sY0FBYyxDQUFDO0FBTW5ELE1BQU0sT0FBTyxlQUFlOzs7Ozs7SUFDMUIsU0FBUyxDQUFDLEtBQXNCLEVBQUUsU0FBaUIsVUFBVTs7WUFDdkQsUUFBUSxHQUFHLE1BQU0sQ0FBQyxLQUFLLElBQUksQ0FBQyxDQUFDO1FBRWpDLE9BQU8sU0FBUyxDQUFDLE1BQU07Ozs7O1FBQUMsQ0FBQyxPQUFPLEVBQUUsQ0FBQyxJQUFJLEVBQUUsSUFBSSxDQUFDLEVBQUUsRUFBRTtZQUNoRCxJQUFJLE9BQU8sQ0FBQyxPQUFPLENBQUMsSUFBSSxDQUFDLEtBQUssQ0FBQyxDQUFDLEVBQUU7O3NCQUMxQixDQUFDLEdBQUcsSUFBSSxDQUFDLEtBQUssQ0FBQyxRQUFRLEdBQUcsSUFBSSxDQUFDO2dCQUNyQyxRQUFRLElBQUksQ0FBQyxHQUFHLElBQUksQ0FBQztnQkFDckIsT0FBTyxPQUFPLENBQUMsT0FBTyxDQUFDLElBQUksTUFBTSxDQUFDLEdBQUcsSUFBSSxHQUFHLEVBQUUsR0FBRyxDQUFDOzs7O2dCQUFFLENBQUMsS0FBYSxFQUFFLEVBQUU7b0JBQ3BFLE9BQU8sUUFBUSxDQUFDLENBQUMsQ0FBQyxRQUFRLEVBQUUsRUFBRSxLQUFLLENBQUMsTUFBTSxFQUFFLEdBQUcsQ0FBQyxDQUFDO2dCQUNuRCxDQUFDLEVBQUMsQ0FBQzthQUNKO1lBQ0QsT0FBTyxPQUFPLENBQUM7UUFDakIsQ0FBQyxHQUFFLE1BQU0sQ0FBQyxDQUFDO0lBQ2IsQ0FBQzs7O1lBbEJGLElBQUksU0FBQztnQkFDSixJQUFJLEVBQUUsYUFBYTtnQkFDbkIsSUFBSSxFQUFFLElBQUk7YUFDWCIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7IFBpcGUsIFBpcGVUcmFuc2Zvcm0gfSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7IHBhZFN0YXJ0LCB0aW1lVW5pdHMgfSBmcm9tICcuLi9jb3JlL3V0aWwnO1xuXG5AUGlwZSh7XG4gIG5hbWU6ICduelRpbWVSYW5nZScsXG4gIHB1cmU6IHRydWVcbn0pXG5leHBvcnQgY2xhc3MgTnpUaW1lUmFuZ2VQaXBlIGltcGxlbWVudHMgUGlwZVRyYW5zZm9ybSB7XG4gIHRyYW5zZm9ybSh2YWx1ZTogc3RyaW5nIHwgbnVtYmVyLCBmb3JtYXQ6IHN0cmluZyA9ICdISDptbTpzcycpOiBzdHJpbmcge1xuICAgIGxldCBkdXJhdGlvbiA9IE51bWJlcih2YWx1ZSB8fCAwKTtcblxuICAgIHJldHVybiB0aW1lVW5pdHMucmVkdWNlKChjdXJyZW50LCBbbmFtZSwgdW5pdF0pID0+IHtcbiAgICAgIGlmIChjdXJyZW50LmluZGV4T2YobmFtZSkgIT09IC0xKSB7XG4gICAgICAgIGNvbnN0IHYgPSBNYXRoLmZsb29yKGR1cmF0aW9uIC8gdW5pdCk7XG4gICAgICAgIGR1cmF0aW9uIC09IHYgKiB1bml0O1xuICAgICAgICByZXR1cm4gY3VycmVudC5yZXBsYWNlKG5ldyBSZWdFeHAoYCR7bmFtZX0rYCwgJ2cnKSwgKG1hdGNoOiBzdHJpbmcpID0+IHtcbiAgICAgICAgICByZXR1cm4gcGFkU3RhcnQodi50b1N0cmluZygpLCBtYXRjaC5sZW5ndGgsICcwJyk7XG4gICAgICAgIH0pO1xuICAgICAgfVxuICAgICAgcmV0dXJuIGN1cnJlbnQ7XG4gICAgfSwgZm9ybWF0KTtcbiAgfVxufVxuIl19